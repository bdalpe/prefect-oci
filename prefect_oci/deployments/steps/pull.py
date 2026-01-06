import logging
import os

logger = logging.getLogger(__name__)


async def pull_oci_image(
    name: str,
    tag: str,
    path: str | None = os.getcwd(),
    client_kwargs: dict | None = None,
):
    """
    Copies an OCI image from a remote registry to a local directory.
    
    Extracts the image layers into the target directory.
    
    :param name: 
    :param tag: 
    :param path:
    :param client_kwargs:
    :return: 
    """
    from prefect_oci.provider.registry import Registry

    client = Registry(**client_kwargs or {})

    logger.info("Pulling OCI image %s:%s to %s", name, tag, path)

    files = client.pull(
        "{0}:{1}".format(name, tag),
        outdir=path
    )
    logger.info("Successfully pulled OCI image %s:%s (%d file(s))", name, tag, len(files))
    logger.debug("Extracted files: %s", files)

    return {
        "files": files,
        "path": path,
    }
