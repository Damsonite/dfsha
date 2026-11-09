MAX_COMPONENT_LENGTH = 255
MAX_PATH_LENGTH = 4096


class InvalidPath(ValueError):
    pass


def normalize_path(path):
    if path is None:
        raise InvalidPath("Path is required")

    if "\x00" in path:
        raise InvalidPath("Path contains a NUL character")

    if len(path) > MAX_PATH_LENGTH:
        raise InvalidPath(f"Path exceeds {MAX_PATH_LENGTH} characters")

    if not path.startswith("/"):
        path = "/" + path

    components = []

    for component in path.split("/"):
        if component in ("", "."):
            continue

        if component == "..":
            if components:
                components.pop()
            continue

        if len(component) > MAX_COMPONENT_LENGTH:
            raise InvalidPath(
                f"Path component exceeds {MAX_COMPONENT_LENGTH} characters"
            )

        components.append(component)

    return "/" + "/".join(components)


def join_path(cwd, path):
    if not path:
        return normalize_path(cwd)

    if path.startswith("/"):
        return normalize_path(path)

    base = normalize_path(cwd).rstrip("/")

    return normalize_path(f"{base}/{path}")


def split_parent(path):
    normalized = normalize_path(path)

    if normalized == "/":
        return "/", None

    parent, _, name = normalized.rpartition("/")

    if not parent:
        parent = "/"

    return parent, name
