import argparse
import sys

import grpc

from src.client.dfs_client import DFSClient, DownloadIntegrityError


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

    mkdir_parser = subparsers.add_parser("mkdir")

    mkdir_parser.add_argument("name")
    mkdir_parser.add_argument("--parent", default="")
    mkdir_parser.add_argument("--user", default="user-001")

    ls_parser = subparsers.add_parser("ls")

    ls_parser.add_argument("--parent", default="")
    ls_parser.add_argument("--user", default="user-001")

    stat_parser = subparsers.add_parser("stat")

    stat_parser.add_argument("entry_id")
    stat_parser.add_argument("--user", default="user-001")

    rm_parser = subparsers.add_parser("rm")

    rm_parser.add_argument("entry_id")
    rm_parser.add_argument("--user", default="user-001")

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

        elif args.command == "mkdir":
            entry = client.mkdir(args.name, args.user, args.parent)

            print("\nDirectorio creado correctamente")
            print(f"ID: {entry.entry_id}")
            print(f"Nombre: {entry.name}")
            print(f"Tipo: {entry.type}")

        elif args.command == "ls":
            listing = client.list_dir(args.user, args.parent)

            print(f"\nEntradas: {len(listing.entries)}")

            for entry in listing.entries:
                print(
                    f"{entry.type:<9} {entry.size:>10}  "
                    f"{entry.name}  {entry.entry_id}"
                )

        elif args.command == "stat":
            entry = client.stat(args.entry_id, args.user)

            print("\nInformación de la entrada")
            print(f"ID: {entry.entry_id}")
            print(f"Nombre: {entry.name}")
            print(f"Tipo: {entry.type}")
            print(f"Tamaño: {entry.size} bytes")
            print(f"Checksum: {entry.checksum}")
            print(f"Versión: {entry.version}")

        elif args.command == "rm":
            result = client.remove(args.entry_id, args.user)

            print(f"\nEntrada eliminada: {result.message}")

    except grpc.RpcError as error:
        print(
            f"Error: {error.code().name}: {error.details()}",
            file=sys.stderr,
        )
        sys.exit(1)

    except (OSError, DownloadIntegrityError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

    finally:
        client.close()


if __name__ == "__main__":
    main()
