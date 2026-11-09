import argparse
import shlex
import sys
from cmd import Cmd
from uuid import UUID

import grpc

from src.client.dfs_client import DFSClient, DownloadIntegrityError
from src.common.path_utils import InvalidPath, join_path


def is_entry_id(value):
    try:
        UUID(value)
    except (AttributeError, ValueError):
        return False

    return True


class DFSShell(Cmd):
    intro = (
        "DFSha shell. Escribe 'help' para ver los comandos "
        "y 'quit' para salir."
    )
    prompt = "dfsha> "

    def __init__(self, client, user):
        super().__init__()
        self.client = client
        self.user = user
        self.cwd = "/"
        self.handle = None
        self.last_lease = None

    def _resolve(self, path):
        return join_path(self.cwd, path)

    @staticmethod
    def _split(arg):
        return shlex.split(arg)

    def _fail(self, error):
        if isinstance(error, grpc.RpcError):
            print(
                f"Error: {error.code().name}: {error.details()}",
                file=sys.stderr,
            )
            return

        print(f"Error: {error}", file=sys.stderr)

    def _require_no_handle(self):
        if self.handle is not None and not self.handle.closed:
            raise RuntimeError(
                "Close the open file handle before using path commands"
            )

    def do_pwd(self, arg):
        """pwd: muestra el directorio actual."""
        print(self.cwd)

    def do_cd(self, arg):
        """cd <ruta>: cambia el directorio actual."""
        try:
            parts = self._split(arg)
            target = self._resolve(parts[0]) if parts else "/"
            entry = self.client.stat_path(target, self.user)

            if entry.type != "DIRECTORY":
                raise NotADirectoryError(f"Not a directory: {target}")

            self.cwd = target
        except (grpc.RpcError, InvalidPath, OSError) as error:
            self._fail(error)

    def do_ls(self, arg):
        """ls [ruta]: lista un directorio."""
        try:
            parts = self._split(arg)
            target = self._resolve(parts[0]) if parts else self.cwd
            listing = self.client.list_dir_path(target, self.user)

            print(f"Entradas: {len(listing.entries)}")

            for entry in listing.entries:
                print(
                    f"{entry.type:<9} {entry.size:>10}  "
                    f"{entry.name}  {entry.entry_id}"
                )
        except (grpc.RpcError, InvalidPath, OSError) as error:
            self._fail(error)

    def do_mkdir(self, arg):
        """mkdir [-p] <ruta>: crea un directorio."""
        try:
            self._require_no_handle()
            parts = self._split(arg)
            parents = "-p" in parts
            parts = [part for part in parts if part != "-p"]

            if not parts:
                raise ValueError("mkdir requires a path")

            target = self._resolve(parts[0])
            entry = self.client.mkdir_path(
                target, self.user, create_parents=parents
            )
            print(f"Directorio listo: {entry.entry_id}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            self._fail(error)

    def do_rmdir(self, arg):
        """rmdir <ruta>: elimina un directorio vacío."""
        try:
            self._require_no_handle()
            parts = self._split(arg)

            if not parts:
                raise ValueError("rmdir requires a path")

            target = self._resolve(parts[0])
            entry = self.client.stat_path(target, self.user)

            if entry.type != "DIRECTORY":
                raise NotADirectoryError(f"Not a directory: {target}")

            if not entry.entry_id:
                raise InvalidPath("Cannot remove the root directory")

            result = self.client.remove_path(target, self.user)
            print(f"Directorio eliminado: {result.message}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            self._fail(error)

    def do_rm(self, arg):
        """rm <ruta>: elimina un archivo o un directorio vacío."""
        try:
            self._require_no_handle()
            parts = self._split(arg)

            if not parts:
                raise ValueError("rm requires a path")

            result = self.client.remove_path(
                self._resolve(parts[0]), self.user
            )
            print(f"Entrada eliminada: {result.message}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            self._fail(error)

    def do_put(self, arg):
        """put <archivo_local> [ruta_remota]: sube un archivo."""
        try:
            parts = self._split(arg)

            if not parts:
                raise ValueError("put requires a local file")

            remote = self._resolve(parts[1]) if len(parts) > 1 else self.cwd
            response = self.client.upload_path(
                parts[0], remote, self.user
            )
            print(f"Archivo cargado: {response.file_id}")
        except (grpc.RpcError, InvalidPath, OSError) as error:
            self._fail(error)

    def do_get(self, arg):
        """get <ruta_remota|id> <destino_local>: descarga un archivo."""
        try:
            parts = self._split(arg)

            if len(parts) != 2:
                raise ValueError(
                    "get requires a remote path and a destination"
                )

            target = (
                parts[0]
                if is_entry_id(parts[0])
                else self._resolve(parts[0])
            )

            if is_entry_id(target):
                result = self.client.download(
                    target, self.user, parts[1]
                )
            else:
                result = self.client.download_path(
                    target, self.user, parts[1]
                )

            print(f"Archivo descargado: {result['checksum']}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            DownloadIntegrityError,
        ) as error:
            self._fail(error)

    def do_stat(self, arg):
        """stat <ruta|id>: muestra los metadatos de una entrada."""
        try:
            parts = self._split(arg)

            if not parts:
                raise ValueError("stat requires a path or ID")

            if is_entry_id(parts[0]):
                entry = self.client.stat(parts[0], self.user)
            else:
                entry = self.client.stat_path(
                    self._resolve(parts[0]), self.user
                )

            print(f"ID: {entry.entry_id}")
            print(f"Nombre: {entry.name}")
            print(f"Tipo: {entry.type}")
            print(f"Tamaño: {entry.size} bytes")
            print(f"Checksum: {entry.checksum}")
            print(f"Versión: {entry.version}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            self._fail(error)

    def do_open(self, arg):
        """open <ruta> [r|w]: abre un archivo para lectura o escritura."""
        try:
            parts = self._split(arg)

            if not parts:
                raise ValueError("open requires a path")

            if self.handle is not None and not self.handle.closed:
                raise RuntimeError("A file handle is already open")

            mode = parts[1] if len(parts) > 1 else "r"

            self.handle = self.client.open(
                self._resolve(parts[0]), mode, self.user
            )
            print(f"Handle abierto en modo {mode}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
            DownloadIntegrityError,
        ) as error:
            self._fail(error)

    def do_read(self, arg):
        """read [bytes]: lee del handle abierto."""
        try:
            if self.handle is None or self.handle.closed:
                raise RuntimeError("No file handle is open")

            parts = self._split(arg)
            size = int(parts[0]) if parts else -1
            data = self.handle.read(size)
            print(data.decode("utf-8", errors="backslashreplace"))
        except (
            grpc.RpcError,
            OSError,
            ValueError,
            RuntimeError,
            DownloadIntegrityError,
        ) as error:
            self._fail(error)

    def do_write(self, arg):
        """write <texto>: escribe texto en el handle abierto."""
        try:
            if self.handle is None or self.handle.closed:
                raise RuntimeError("No file handle is open")

            written = self.handle.write(arg.encode("utf-8"))
            print(f"{written} bytes escritos")
        except (grpc.RpcError, OSError, ValueError, RuntimeError) as error:
            self._fail(error)

    def do_close(self, arg):
        """close: cierra el handle abierto y confirma la escritura."""
        try:
            if self.handle is None or self.handle.closed:
                print("No file handle is open")
                return

            response = self.handle.close()
            self.handle = None

            if response is not None:
                print(f"Archivo confirmado: {response.file_id}")
            else:
                print("Handle cerrado")
        except (grpc.RpcError, OSError, ValueError) as error:
            self.handle = None
            self._fail(error)

    def do_lock(self, arg):
        """lock <ruta>: reserva un nombre para escritura."""
        try:
            parts = self._split(arg)

            if not parts:
                raise ValueError("lock requires a path")

            self.last_lease = self.client.lock(
                self._resolve(parts[0]), self.user
            )
            print(f"Lock adquirido: {self.last_lease}")
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            self._fail(error)

    def do_unlock(self, arg):
        """unlock [lease_id]: libera el último lock o el indicado."""
        try:
            parts = self._split(arg)
            lease_id = parts[0] if parts else self.last_lease

            if not lease_id:
                raise ValueError("No lock to release")

            result = self.client.unlock(lease_id, self.user)
            self.last_lease = None
            print(result.message)
        except (
            grpc.RpcError,
            InvalidPath,
            OSError,
            ValueError,
            RuntimeError,
        ) as error:
            self._fail(error)

    def do_quit(self, arg):
        """quit: cierra el shell."""
        if self.handle is not None and not self.handle.closed:
            self.handle.close()

        return True

    def do_EOF(self, arg):
        print()
        return self.do_quit(arg)


def run_shell(client, user):
    DFSShell(client, user).cmdloop()


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

    rmdir_parser = subparsers.add_parser("rmdir")

    rmdir_parser.add_argument("path")
    rmdir_parser.add_argument("--user", default="user-001")

    shell_parser = subparsers.add_parser("shell")

    shell_parser.add_argument("--user", default="user-001")

    args = parser.parse_args()

    client = DFSClient()

    try:
        if args.command == "shell":
            run_shell(client, args.user)
            return

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

        elif args.command == "rmdir":
            entry = client.stat_path(args.path, args.user)

            if entry.type != "DIRECTORY":
                print(
                    f"Error: not a directory: {args.path}", file=sys.stderr
                )
                sys.exit(1)

            if not entry.entry_id:
                print(
                    "Error: cannot remove the root directory",
                    file=sys.stderr,
                )
                sys.exit(1)

            result = client.remove_path(args.path, args.user)

            print(f"\nDirectorio eliminado: {result.message}")

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
