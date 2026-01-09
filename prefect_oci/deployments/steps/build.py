import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional, List, Iterable

from prefect.utilities.filesystem import filter_files
from prefect.utilities.processutils import run_process

from prefect_oci.deployments.logging import LoggerWriter
from prefect_oci.utils.archive import make_targz

logger = logging.getLogger(__name__)


async def create_tar_archive(
    sources: str | List[str],
    output_path: str | None = None,
    archive_root: str | None = None,
    working_directory: str = os.getcwd(),
    ignore_file: Optional[str] = ".prefectignore",
) -> dict:
    """
    Creates a tar.gz archive of the specified source directory.

    :param sources: The items or directories to archive.
    :param output_path: The path where the archive will be saved.
    :return: Path to the created archive.
    :param archive_root: Path prefix inside the archive. Files will be stored
            under this path so they extract into this directory.
    :param working_directory: The working directory to use when creating the archive.
    :param ignore_file: Path to a file containing ignore patterns (like .gitignore).
    """
    output_path = output_path or tempfile.NamedTemporaryFile(suffix=".tar.gz").name
    logger.info("Creating tar archive at %s", output_path)

    included_files = None
    if ignore_file and Path(ignore_file).exists():
        logger.debug("Using ignore file: %s", ignore_file)
        with open(ignore_file, "r") as f:
            ignore_patterns = f.readlines()

        included_files = filter_files(str(working_directory), ignore_patterns)
        logger.debug("Filtered %d files using ignore patterns", len(included_files))
    
    sources = [sources] if isinstance(sources, str) else sources
    logger.debug("Archiving %d source(s): %s", len(sources), sources)

    def item_generator() -> Iterable[Path]:
        cwd = Path(working_directory)

        for source in sources:
            source_path = Path(source)
            if not source_path.is_absolute():
                source_path = cwd / source_path

            candidates = source_path.rglob("*") if source_path.is_dir() else [source_path]
            for path in candidates:
                if path.is_file():
                    if included_files is not None and str(path.relative_to(cwd)) not in included_files:
                        continue

                    if source_path.is_dir():
                        logger.debug("Including file in archive: %s", path.relative_to(cwd))
                    yield path
    
    make_targz(
        item_generator(),
        output_path,
        working_directory=working_directory,
        archive_root=archive_root
    )
    logger.info("Successfully created tar archive at %s", output_path)

    return {
        "output_path": output_path
    }



async def install_dependencies_for_archiving(
        requirements_file: str,
        target_directory: str,
        platform: str | None = None,
        additional_pip_args: list[str] | None = None,
        stream_output: bool | tuple = True,
):
    """
    Install packages from a requirements.txt file into a target directory.

    Detects if `uv` is available and uses it for installation, 
    falling back to using the standard `pip` module if not.

    :param requirements_file: Path to the requirements.txt file.
    :param target_directory: Directory to install the packages into.
    :param platform: Optional platform specifier for pip installation. 
        See https://docs.astral.sh/uv/reference/cli/#uv-pip-install--python-platform
        for a list of supported platforms.
    :param additional_pip_args: Additional arguments to pass to pip.
    :param stream_output: Whether to stream the output to logger.
    """
    logger.info("Installing dependencies from %s to %s", requirements_file, target_directory)

    command = ["pip3", "install", "-r", requirements_file, "--target", target_directory]

    if platform:
        logger.debug("Target platform: %s", platform)
        command.extend(["--python-platform", platform])

    if additional_pip_args:
        command.extend(additional_pip_args)

    uv = shutil.which("uv")

    if uv:
        command = [uv, *command]
        tool = "uv"
    else:
        command = [sys.executable, "-m", *command]
        tool = "pip"

    logger.debug("Using %s for dependency installation", tool)
    logger.debug("Command: %s", " ".join(command))

    if stream_output == True:
        stream_output = (LoggerWriter(logger, logging.INFO), LoggerWriter(logger, logging.ERROR))

    await run_process(
        command,
        stream_output=stream_output
    )
    logger.info("Successfully installed dependencies to %s", target_directory)
