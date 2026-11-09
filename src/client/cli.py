import argparse
import sys
from uuid import UUID

import grpc

from src.client.dfs_client import DFSClient, DownloadIntegrityError
from src.common.path_utils import InvalidPath


def is_entry_id(value):
    try:
        UUID(value)
    except (AttributeError, ValueError):
        return False

    return True


def main():

    parser = argparse.ArgumentParser(description="DFSha - Distributed File System")

    subparsers = parser.add_subparsers(dest="command", required=True)

    put_parser = subparsers.add_parser("put")

    put_parser.add_argument("file")
    put_parser.add_argument("remote_path", nargs="?")
    put_parser.add_argument("--user", default="user-001")
    put_parser.add_argument("--parent", default="")

    get_parser = subparsers.add_parser("get")

    get_parser.add_argument("target")
    get_parser.add_argument("destination")
    get_parser.add_argument("--user", default="user-001")

    mkdir_parser = subparsers.add_parser("mkdir")

    mkdir_parser.add_argument("path")
    mkdir_parser.add_argument("--parent", default="")
    mkdir_parser.add_argument("-p", "--parents", action="store_true")
    mkdir_parser.add_argument("--user", default="user-001")

    ls_parser = subparsers.add_parser("ls")

    ls_parser.add_argument("path", nargs="?")
    ls_parser.add_argument("--parent", default="")
    ls_parser.add_argument("--user", default="user-001")

    stat_parser = subparsers.add_parser("stat")

    stat_parser.add_argument("target")
    stat_parser.add_argument("--user", default="user-001")

    rm_parser = subparsers.add_parser("rm")

    rm_parser.add_argument("target")
    rm_parser.add_argument("--user", default="user-001")

    args = parser.parse_args()

    client = DFSClient()

    try:
        if args.command == "put":
            if args.remote_path and args.parent:
                print(
                    "Error: use --parent or a remote path, not both",
                    file=sys.stderr,
                )
                sys.exit(2)

            if args.remote_path:
                response = client.upload_path(
                    args.file, args.remote_path, args.user
                )
            else:
                response = client.upload(args.file, args.user, args.parent)

            print("\nArchivo cargado correctamente")
            print(f"ID: {response.file_id}")
            print(f"Nombre: {response.filename}")
            print(f"Tamaño: {response.size} bytes")
            print(f"Checksum: {response.checksum}")
            print(f"Bloques: {response.block_count}")

        elif args.command == "get":
            if is_entry_id(args.target):
                result = client.download(
                    args.target, args.user, args.destination
                )
            else:
                result = client.download_path(
                    args.target, args.user, args.destination
                )

            print("\nArchivo descargado correctamente")
            print(f"Ruta: {result['path']}")
            print(f"SHA-256: {result['checksum']}")

        elif args.command == "mkdir":
            if args.parent and args.parents:
                print(
                    "Error: --parent cannot be combined with --parents",
                    file=sys.stderr,
                )
                sys.exit(2)

            if args.parent:
                entry = client.mkdir(args.path, args.user, args.parent)
            else:
                entry = client.mkdir_path(
                    args.path,
                    args.user,
                    create_parents=args.parents,
                )

            print("\nDirectorio listo")
            print(f"ID: {entry.entry_id}")
            print(f"Nombre: {entry.name}")
            print(f"Tipo: {entry.type}")
            print(f"Mensaje: {entry.message}")

        elif args.command == "ls":
            if args.parent and args.path:
                print(
                    "Error: use --parent or a path, not both",
                    file=sys.stderr,
                )
                sys.exit(2)

            if args.parent:
                listing = client.list_dir(args.user, args.parent)
            elif args.path:
                listing = client.list_dir_path(args.path, args.user)
            else:
                listing = client.list_dir(args.user)

            print(f"\nEntradas: {len(listing.entries)}")

            for entry in listing.entries:
                print(
                    f"{entry.type:<9} {entry.size:>10}  "
                    f"{entry.name}  {entry.entry_id}"
                )

        elif args.command == "stat":
            if is_entry_id(args.target):
                entry = client.stat(args.target, args.user)
            else:
                entry = client.stat_path(args.target, args.user)

            print("\nInformación de la entrada")
            print(f"ID: {entry.entry_id}")
            print(f"Nombre: {entry.name}")
            print(f"Tipo: {entry.type}")
            print(f"Tamaño: {entry.size} bytes")
            print(f"Checksum: {entry.checksum}")
            print(f"Versión: {entry.version}")

        elif args.command == "rm":
            if is_entry_id(args.target):
                result = client.remove(args.target, args.user)
            else:
                result = client.remove_path(args.target, args.user)

            print(f"\nEntrada eliminada: {result.message}")

    except grpc.RpcError as error:
        print(
            f"Error: {error.code().name}: {error.details()}",
            file=sys.stderr,
        )
        sys.exit(1)

    except InvalidPath as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

    except (OSError, DownloadIntegrityError) as error:
        print(f"Error: {error}", file=sys.stderr)
        sys.exit(1)

    finally:
        client.close()


if __name__ == "__main__":
    main()
