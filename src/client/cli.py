import argparse
import sys

import grpc

from src.client.dfs_client import DFSClient


def main():

    parser = argparse.ArgumentParser(description="DFSha - Distributed File System")

    subparsers = parser.add_subparsers(dest="command", required=True)

    put_parser = subparsers.add_parser("put")

    put_parser.add_argument("file")
    put_parser.add_argument("--user", default="user-001")
    put_parser.add_argument("--parent", default="")

    get_parser = subparsers.add_parser("get")

    get_parser.add_argument("file_id")
    get_parser.add_argument("destination")
    get_parser.add_argument("--user", default="user-001")

    args = parser.parse_args()

    client = DFSClient()

    try:
        if args.command == "put":
            response = client.upload(args.file, args.user, args.parent)

            print("\nArchivo cargado correctamente")
            print(f"ID: {response.file_id}")
            print(f"Nombre: {response.filename}")
            print(f"Tamaño: {response.size} bytes")
            print(f"Checksum: {response.checksum}")
            print(f"Bloques: {response.block_count}")

        elif args.command == "get":
            result = client.download(args.file_id, args.user, args.destination)

            print("\nArchivo descargado correctamente")
            print(f"Ruta: {result['path']}")
            print(f"SHA-256: {result['checksum']}")

    except grpc.RpcError as error:
        print(
            f"Error: {error.code().name}: {error.details()}",
            file=sys.stderr,
        )
        sys.exit(1)

    except OSError as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

    finally:
        client.close()


if __name__ == "__main__":
    main()
