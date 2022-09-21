import pathlib


def walk(path):
    """Collects paths for files in path parameter

    Parameters
    ----------
    path: str or pathlib.Path() object
            Path to metadata folder containing IDR study directories

    Returns
    -------
    PosixPath object
    """
    for subdir in pathlib.Path(path).iterdir():
        if subdir.is_dir():
            yield from walk(subdir)
            continue
        yield subdir.resolve()
