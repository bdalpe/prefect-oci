import logging
import os

logging.basicConfig(level=logging.DEBUG)
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
    
    logger.debug("Pulling OCI image %s:%s to temporary directory %s", name, tag, path)
        
    files = client.pull(
        "{0}:{1}".format(name, tag),
        outdir=path
    )
    
    return {
        "files": files,
        "path": path,
    }
