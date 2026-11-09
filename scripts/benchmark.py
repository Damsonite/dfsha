import argparse
import hashlib
import json
import platform
import statistics
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.client.dfs_client import DFSClient
from src.common import metadata_pb2

METADATA_ENDPOINT = "localhost:50051"
DATA_ENDPOINT = "localhost:50052"
OWNER_ID = "user-001"
MIB = 1024 * 1024


def percentile(values, fraction):
    ordered = sorted(values)
    index = int(fraction * (len(ordered) - 1))
    return ordered[index]


def summarize_seconds(values):
    milliseconds = [value * 1000 for value in values]

    return {
        "runs": len(values),
        "avg_ms": round(statistics.mean(milliseconds), 3),
        "p50_ms": round(percentile(milliseconds, 0.50), 3),
        "p95_ms": round(percentile(milliseconds, 0.95), 3),
        "min_ms": round(min(milliseconds), 3),
        "max_ms": round(max(milliseconds), 3),
    }


def measure_metadata(client, owner_id, root_path, entry_id, runs):
    operations = {
        "stat": lambda: client.metadata_stub.Stat(
            metadata_pb2.StatRequest(
                owner_id=owner_id, entry_id=entry_id
            ),
            timeout=30,
        ),
        "resolve_path": lambda: client.metadata_stub.ResolvePath(
            metadata_pb2.ResolvePathRequest(
                owner_id=owner_id, path=root_path
            ),
            timeout=30,
        ),
        "list_dir": lambda: client.metadata_stub.ListDir(
            metadata_pb2.ListDirRequest(
                owner_id=owner_id, parent_id=""
            ),
            timeout=30,
        ),
    }

    results = {}

    for name, operation in operations.items():
        for _ in range(2):
            operation()

        durations = []

        for _ in range(runs):
            started = time.perf_counter()
            operation()
            durations.append(time.perf_counter() - started)

        results[name] = summarize_seconds(durations)

    return results


def make_payload(size_bytes):
    pattern = b"DFSha benchmark payload\n"
    repetitions = size_bytes // len(pattern) + 1
    return (pattern * repetitions)[:size_bytes]


def benchmark_file(client, owner_id, parent_id, temporary_path, size_bytes, runs):
    upload_times = []
    download_times = []
    block_counts = []

    for run in range(runs):
        payload = make_payload(size_bytes)
        expected_checksum = hashlib.sha256(payload).hexdigest()

        source = temporary_path / f"benchmark-{size_bytes}-{run}.bin"
        source.write_bytes(payload)
        destination = temporary_path / f"benchmark-{size_bytes}-{run}.out"
        uploaded = None

        try:
            started = time.perf_counter()
            uploaded = client.upload(source, owner_id, parent_id)
            upload_times.append(time.perf_counter() - started)

            started = time.perf_counter()
            downloaded = client.download(
                uploaded.file_id, owner_id, destination
            )
            download_times.append(time.perf_counter() - started)

            if downloaded["size"] != size_bytes:
                raise AssertionError(
                    f"downloaded size {downloaded['size']} != {size_bytes}"
                )

            if downloaded["checksum"] != expected_checksum:
                raise AssertionError("downloaded checksum mismatch")

            block_counts.append(uploaded.block_count)
        finally:
            if uploaded is not None:
                client.remove(uploaded.file_id, owner_id)

            source.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)

    average_upload = statistics.mean(upload_times)
    average_download = statistics.mean(download_times)

    return {
        "size_bytes": size_bytes,
        "size_mib": round(size_bytes / MIB, 3),
        "blocks": block_counts,
        "upload": {
            **summarize_seconds(upload_times),
            "avg_mib_s": round((size_bytes / MIB) / average_upload, 3),
        },
        "download": {
            **summarize_seconds(download_times),
            "avg_mib_s": round((size_bytes / MIB) / average_download, 3),
        },
    }


def parse_sizes(raw_value):
    sizes = []

    for item in raw_value.split(","):
        item = item.strip()

        if not item:
            continue

        try:
            size_mib = int(item)
        except ValueError as error:
            raise argparse.ArgumentTypeError(
                f"Invalid size in MiB: {item!r}"
            ) from error

        if size_mib <= 0:
            raise argparse.ArgumentTypeError(
                f"Size must be greater than zero: {item!r}"
            )

        sizes.append(size_mib)

    if not sizes:
        raise argparse.ArgumentTypeError("At least one size is required")

    return sizes


def print_summary(report):
    print("\nMetadata latency")
    print("operation       runs   avg ms   p50 ms   p95 ms   max ms")

    for name, values in report["metadata"].items():
        print(
            f"{name:<14} {values['runs']:>5} "
            f"{values['avg_ms']:>8.3f} {values['p50_ms']:>8.3f} "
            f"{values['p95_ms']:>8.3f} {values['max_ms']:>8.3f}"
        )

    print("\nFile throughput")
    print("size MiB   blocks   upload s   upload MiB/s   download s   download MiB/s")

    for item in report["files"]:
        blocks = item["blocks"][0] if item["blocks"] else 0

        print(
            f"{item['size_mib']:>8.2f} {blocks:>8} "
            f"{item['upload']['avg_ms'] / 1000:>10.3f} "
            f"{item['upload']['avg_mib_s']:>14.3f} "
            f"{item['download']['avg_ms'] / 1000:>12.3f} "
            f"{item['download']['avg_mib_s']:>16.3f}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="DFSha integration benchmark"
    )

    parser.add_argument(
        "--sizes",
        default="1,4,8,16",
        help="Comma-separated file sizes in MiB",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Upload/download runs per file size",
    )
    parser.add_argument(
        "--metadata-runs",
        type=int,
        default=30,
        help="Samples per metadata operation",
    )
    parser.add_argument(
        "--output",
        default="reports/benchmark.json",
        help="JSON report path",
    )

    args = parser.parse_args()

    if args.runs <= 0 or args.metadata_runs <= 0:
        parser.error("--runs and --metadata-runs must be greater than zero")

    try:
        sizes = parse_sizes(args.sizes)
    except argparse.ArgumentTypeError as error:
        parser.error(str(error))

    client = DFSClient(DATA_ENDPOINT, METADATA_ENDPOINT)
    uploaded_ids = []
    root_path = f"/benchmark-{uuid.uuid4().hex[:8]}"

    try:
        root = client.mkdir_path(root_path, OWNER_ID)

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)

            probe = temporary_path / "metadata-probe.bin"
            probe.write_bytes(b"metadata probe")

            probe_upload = client.upload(probe, OWNER_ID, root.entry_id)
            uploaded_ids.append(probe_upload.file_id)

            print(
                f"Benchmarking metadata with {args.metadata_runs} samples..."
            )

            metadata_results = measure_metadata(
                client,
                OWNER_ID,
                root_path,
                probe_upload.file_id,
                args.metadata_runs,
            )

            client.remove(probe_upload.file_id, OWNER_ID)
            uploaded_ids.remove(probe_upload.file_id)

            file_results = []

            for size_mib in sizes:
                print(
                    f"Benchmarking {size_mib} MiB "
                    f"({args.runs} run(s))..."
                )

                result = benchmark_file(
                    client,
                    OWNER_ID,
                    root.entry_id,
                    temporary_path,
                    size_mib * MIB,
                    args.runs,
                )

                file_results.append(result)

        client.remove_path(root_path, OWNER_ID)

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "environment": {
                "python": platform.python_version(),
                "platform": platform.platform(),
                "hostname": platform.node(),
            },
            "configuration": {
                "block_size_bytes": 4 * MIB,
                "replication_factor": 3,
                "write_quorum": 2,
                "virtual_nodes": 150,
            },
            "metadata": metadata_results,
            "files": file_results,
        }

        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )

        print_summary(report)
        print(f"\nJSON report written to {output_path}")

    finally:
        for file_id in uploaded_ids:
            try:
                client.remove(file_id, OWNER_ID)
            except Exception as error:
                print(f"Cleanup failed for {file_id}: {error}")

        try:
            client.remove_path(root_path, OWNER_ID)
        except Exception:
            pass

        client.close()


if __name__ == "__main__":
    main()
