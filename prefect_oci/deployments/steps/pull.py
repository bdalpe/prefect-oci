import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)


async def pull_oci_image(
    name: str,
    tag: str,
    path: Optional[str] | None = None,
    credentials: Optional[dict[str, Any]] = None,
    client_kwargs: Optional[dict] = None,
):
    """
    Copies an OCI image from a remote registry to a local directory.
    
    Extracts the image layers into the target directory.
    
    :param name: The name of the OCI image.
    :param tag: The tag of the OCI image.
    :param path: The local directory path to extract layers to.
    :param credentials: Optional Prefect block reference or block object for authentication.
                       Use DockerRegistryCredentials for standard registries or AwsCredentials for ECR.
    :param client_kwargs: Optional keyword arguments to pass to the Registry client.
    :return: Dictionary with "files" (list of extracted files) and "path" (extraction directory).
    """
    from prefect_oci.provider.auth import resolve_credentials
    from prefect_oci.provider.container import Container
    from prefect_oci.provider.registry import Registry
    
    path = path or os.getcwd()
    
    container = Container(f"{name}:{tag}")

    username, password, registry_url, auth_backend = resolve_credentials(credentials, container.registry)

    client_kwargs = (client_kwargs or {}).copy()
    if credentials:
        client_kwargs['auth_backend'] = auth_backend

    # Prepare client kwargs with authentication
    client = Registry(**client_kwargs)
    
    # Login if credentials resolved
    if username and password:
        logger.debug("Logging in to registry: %s", registry_url or "default")
        client.login(
            username=username, 
            password=password, 
            hostname=registry_url
        )

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
