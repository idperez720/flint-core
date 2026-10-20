"""Owned local directory staging with atomic, exclusive final-name publication."""

import ctypes
import errno
import os
import stat
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from flint_core.runtime.errors import DataValidationError, PublicationConflict, PublicationUncertain


@dataclass(frozen=True)
class PublicationReceipt:
    cleanup_pending: bool = False


def dataset_path(snapshot, binding):
    path = binding.target.path
    root = snapshot.project_root
    if not path.is_relative_to(root):
        raise DataValidationError("Target is outside the project root.")
    current = root
    for component in ("", *path.relative_to(root).parts):
        if component:
            current /= component
        if current.is_symlink():
            raise DataValidationError("Dataset paths cannot traverse symlinks.")
    return path


def _rename_function():
    if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
        raise DataValidationError("Atomic Delta create requires a certified local POSIX filesystem.")
    library = ctypes.CDLL(None, use_errno=True)
    name = "renamex_np" if sys.platform == "darwin" else "renameat2" if sys.platform.startswith("linux") else None
    if name is None or not hasattr(library, name):
        raise DataValidationError("This platform has no supported exclusive directory rename.")
    function = getattr(library, name)
    function.restype = ctypes.c_int
    function.argtypes = (
        [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        if name == "renamex_np"
        else [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    )
    return function, name


def rename_exclusive(source, destination):
    function, name = _rename_function()
    arguments = (
        (os.fsencode(source), os.fsencode(destination), 4)
        if name == "renamex_np"
        else (-100, os.fsencode(source), -100, os.fsencode(destination), 1)
    )
    if function(*arguments) != 0:
        code = ctypes.get_errno()
        if code in (errno.EEXIST, errno.ENOTEMPTY):
            raise FileExistsError("The create target already exists.")
        raise OSError(code, "Exclusive directory publication failed.")


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _inventory(path):
    result = {}
    for child in (path, *path.rglob("*")):
        status = child.lstat()
        if not (stat.S_ISREG(status.st_mode) or stat.S_ISDIR(status.st_mode)):
            raise DataValidationError("Delta staging requires only regular files and directories.")
        result[child] = (status.st_dev, status.st_ino, stat.S_IFMT(status.st_mode))
    return result


def _cleanup(stage, identity, inventory):
    try:
        status = stage.lstat()
        if (status.st_dev, status.st_ino) != identity or _inventory(stage) != inventory:
            return True
        for child in sorted(inventory, key=lambda path: len(path.parts), reverse=True):
            status = child.lstat()
            if (status.st_dev, status.st_ino, stat.S_IFMT(status.st_mode)) != inventory[child]:
                return True
            child.rmdir() if stat.S_ISDIR(status.st_mode) else child.unlink()
        return False
    except FileNotFoundError:
        return stage.exists() or stage.is_symlink()
    except (OSError, DataValidationError):
        return True


class DirectoryPublisher:
    def preflight(self, snapshot, dataset):
        _rename_function()
        binding = snapshot.require_binding(dataset, access="write")
        path = dataset_path(snapshot, binding)
        if not path.parent.is_dir():
            raise DataValidationError("Create the output parent directory before running.")
        if path.exists():
            raise PublicationConflict()
        return path

    def _sync_tree(self, inventory):
        for path, identity in inventory.items():
            if identity[2] == stat.S_IFREG:
                descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    status = os.fstat(descriptor)
                    if (status.st_dev, status.st_ino, stat.S_IFMT(status.st_mode)) != identity:
                        raise DataValidationError("A staged file was replaced.")
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        for path in sorted(inventory, key=lambda child: len(child.parts), reverse=True):
            if inventory[path][2] == stat.S_IFDIR:
                sync_directory(path)

    def publish(self, snapshot, dataset, prepare):
        path = self.preflight(snapshot, dataset)
        stage = Path(tempfile.mkdtemp(prefix=".flint-delta-", dir=path.parent))
        status = stage.lstat()
        identity = (status.st_dev, status.st_ino)
        inventory = _inventory(stage)
        uncertain = False
        failure = None
        try:
            prepare(stage)
            inventory = _inventory(stage)
            self._sync_tree(inventory)
            dataset_path(snapshot, snapshot.bindings[dataset])
            if _inventory(stage) != inventory or (stage.stat().st_dev, stage.stat().st_ino) != identity:
                raise DataValidationError("The owned stage changed before publication.")
            try:
                rename_exclusive(stage, path)
            except FileExistsError:
                raise
            except Exception:
                uncertain = True
                raise
            try:
                sync_directory(path.parent)
            except Exception:
                uncertain = True
                raise
        except Exception as error:
            failure = error
        finally:
            if failure is not None and inventory.keys() == {stage}:
                try:
                    current = stage.lstat()
                    if (current.st_dev, current.st_ino) == identity:
                        inventory = _inventory(stage)
                except (OSError, DataValidationError):
                    pass
            pending = True if getattr(failure, "cleanup_pending", False) else _cleanup(stage, identity, inventory)
        if uncertain:
            raise PublicationUncertain(cleanup_pending=pending) from None
        if failure is not None:
            if isinstance(failure, FileExistsError):
                raise PublicationConflict(cleanup_pending=pending) from None
            raise DataValidationError("Delta preparation failed before publication.", cleanup_pending=pending) from None
        return PublicationReceipt(cleanup_pending=pending)
