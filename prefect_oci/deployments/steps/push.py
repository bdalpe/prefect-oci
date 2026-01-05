import json
import logging
import tempfile
from typing import List

from pydantic import BaseModel

from prefect_oci.provider.image import create_oci_image_index_manifest
from prefect_oci.provider.platform import Platform
from prefect_oci.utils.archive import diff_id_from_tar_gz

logger = logging.getLogger(__name__)


class PlatformManifest(BaseModel):
    platform: Platform
    layers: List[str]


async def push_oci_image(
    name: str,
    tag: str,
    layers: List[str] | List[dict],
    client_kwargs: dict | None = None,
):
    """
    Push an OCI manifest or image to a remote registry.
    
    :param name: The name of the OCI image.
    :param tag: The tag (or list of tags) of the OCI image.
    :param layers: A list of file paths or a mapping of platform details to file paths to include as layers.
    :param client_kwargs: Optional keyword arguments to pass to the Registry client.
    """
    from oras.provider import temporary_empty_config
    from prefect_oci.provider.container import Container
    from prefect_oci.provider.oci import EmptyManifestConfig
    from prefect_oci.provider.registry import Registry

    client = Registry(**client_kwargs or {})

    container = Container(f"{name}:{tag}")
    
    logger.info(f"Pushing OCI image {container.api_prefix}:{tag}")
    
    manifests = []
    
    # If layers is a list, we treat it as a single manifest
    if isinstance(layers, list) and all(isinstance(layer, str) for layer in layers):
        response = client.push(
            str(container),
            files=[
                f"{layer}:application/vnd.oci.image.layer.v1.tar+gzip"
                for layer in layers
            ],
            disable_path_validation=True
        )
        
        digest = client.extract_manifest_digest_from_upload_response(response)

        manifest = client.get_manifest(Container.with_new_digest(container, digest))
        manifest['digest'] = digest  # add digest for the index manifest
        manifest['size'] = int(response.headers.get('Content-Length', '0'))
        
        manifests.append(manifest)

    # If layers is a mapping, we treat it as a multi-platform image
    if isinstance(layers, list) and all(isinstance(layer, dict) and PlatformManifest.model_validate(layer) for layer in layers):
        for platform in layers:
            platform: dict
            
            # create config file
            with tempfile.NamedTemporaryFile() as config_file:
                config = {}
                config.update(**platform['platform'])
                config['rootfs'] = {
                    "type": "layers",
                    "diff_ids": [
                        "sha256:{}".format(diff_id_from_tar_gz(layer))
                        for layer in platform['layers']
                    ]
                }
                
                config_file.write(json.dumps(config, indent=None).encode())
                config_file.flush()
            
                response = client.push(
                    str(container),
                    files=[
                        f"{layer}:application/vnd.oci.image.layer.v1.tar+gzip"
                        for layer in platform['layers']
                    ],
                    manifest_config=f"{config_file.name}:application/vnd.oci.image.config.v1+json",
                    disable_path_validation=True
                )
                
                digest = client.extract_manifest_digest_from_upload_response(response)
                
                manifest = client.get_manifest(Container.with_new_digest(container, digest))
                manifest['size'] = len(json.dumps(manifest))
                manifest['digest'] = digest  # add digest for the index manifest
                manifest['platform'] = platform['platform']
                
                manifests.append(manifest)
        
    manifest = create_oci_image_index_manifest(manifests)

    # the image index layer will have an empty config, so we need to ensure this exists
    # even though it is embedded with the manifest
    with temporary_empty_config() as config_file:
        config = EmptyManifestConfig()
        client.upload_blob(config_file, container, config)
    
    # Upload the image index
    image_index_response = client.upload_image_index(
        manifest,
        container=container
    )
    
    digest = client.extract_manifest_digest_from_upload_response(image_index_response)
    
    return {
        "name": name,
        "tag": tag,
        "image": str(container),
        "digest": digest
    }
