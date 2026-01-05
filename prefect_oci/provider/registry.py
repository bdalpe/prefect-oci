from typing import Optional, List

import jsonschema
import oras.container
import oras.defaults
import oras.schemas
import requests
from oras.logger import logger
from oras.provider import Registry as ORASRegistry
from oras import decorator
from oras.types import container_type

from prefect_oci.provider.container import Container
from prefect_oci.provider.defaults import default_image_index_media_type
from prefect_oci.provider.platform import Platform
from prefect_oci.provider.schemas import image_index


class Registry(ORASRegistry):
    def upload_manifest(
            self,
            manifest: dict,
            container: oras.container.Container,
            content_type: str | None = None,
            schema: dict | None = None
    ) -> requests.Response:
        """
        Read a manifest file and upload it.

        :param manifest: manifest to upload
        :type manifest: dict
        :param container: parsed container URI
        :type container: oras.container.Container or str
        :param content_type: optional content type for manifest
        :type content_type: str
        :param schema: optional schema to validate manifest against
        :type schema: dict
        """
        jsonschema.validate(manifest, schema=schema or oras.schemas.manifest)
        headers = {
            "Content-Type": content_type or oras.defaults.default_manifest_media_type,
        }
        return self.do_request(
            f"{self.prefix}://{container.manifest_url()}",  # noqa
            "PUT",
            headers=headers,
            json=manifest,
        )

    def upload_image_index(
            self,
            manifest: dict,
            container: oras.container.Container,
    ) -> requests.Response:
        """
        Wrapper around upload_manifest to upload an image index.
        """
        return self.upload_manifest(
            manifest,
            container,
            content_type=default_image_index_media_type,
            schema=image_index,
        )

    @decorator.ensure_container
    def get_manifest(
        self,
        container: container_type,
        allowed_media_type: Optional[list] = None,
        schema: Optional[dict] = None,
    ) -> dict:
        """
        Retrieve a manifest for a package.

        :param container:  parsed container URI
        :type container: oras.container.Container or str
        :param allowed_media_type: one or more allowed media types
        :type allowed_media_type: str
        :param schema: optional jsonschema to validate against
        :type schema: dict
        """
        # Load authentication configs for the container's registry
        # This ensures credentials are available for authenticated registries
        self.auth.load_configs(container)

        if not allowed_media_type:
            allowed_media_type = [oras.defaults.default_manifest_media_type]
        headers = {"Accept": ";".join(allowed_media_type)}

        get_manifest = f"{self.prefix}://{container.manifest_url()}"  # type: ignore
        response = self.do_request(get_manifest, "GET", headers=headers)

        self._check_200_response(response)
        manifest = response.json()
        jsonschema.validate(manifest, schema=schema or oras.schemas.manifest)
        return manifest

    @decorator.ensure_container
    def get_image_index(
        self,
        container: container_type
    ) -> dict:
        """
        Wrapper around get_manifest to get an image index.

        :param container: parsed container URI
        :type container: oras.container.Container or str
        """

        return self.get_manifest(
            container,
            allowed_media_type=[default_image_index_media_type],
            schema=image_index,
        )

    def pull(
        self,
        target: str,
        config_path: Optional[str] = None,
        allowed_media_type: Optional[List] = None,
        overwrite: bool = True,
        outdir: Optional[str] = None,
    ) -> List[str]:
        """
        Pull an artifact from a target

        :param config_path: path to a config file
        :type config_path: str
        :param allowed_media_type: list of allowed media types
        :type allowed_media_type: list or None
        :param overwrite: if output file exists, overwrite
        :type overwrite: bool
        :param manifest_config_ref: save manifest config to this file
        :type manifest_config_ref: str
        :param outdir: output directory path
        :type outdir: str
        :param target: target location to pull from
        :type target: str
        """
        container = self.get_container(target)
        self.auth.load_configs(
            container, configs=[config_path] if config_path else None
        )

        # Check if the manifest is an image index
        try:
            index = self.get_image_index(container)

            # If multiple manifests match a client or runtime's requirements, 
            # the first matching entry SHOULD be used.
            # https://github.com/opencontainers/image-spec/blob/main/image-index.md

            platform = Platform.detect_system()

            for manifest in index.get("manifests", []):
                if platform.is_match(manifest.get("platform", {})):
                    container = Container.with_new_digest(container, manifest['digest'])
                    break

        except ValueError as e:
            # Image index was not found, continue as normal manifest
            logger.debug(f"Not an image index: {e}")
            pass
        
        # continue with the default pull behavior
        return super().pull(
            str(container),
            config_path=config_path,
            allowed_media_type=allowed_media_type,
            overwrite=overwrite,
            outdir=outdir,
        )
    
    def extract_manifest_digest_from_upload_response(self, response: requests.Response) -> str:
        """
        Extract the manifest digest from a response.

        :param response: HTTP response object
        :type response: requests.Response
        :return: manifest digest
        :rtype: str
        """
        self._check_200_response(response)
            
        # Fallback: use the location header if Docker-Content-Digest is not present
        if "Location" in response.headers:
            location = response.headers["Location"]
            digest = location.split("/")[-1]
            logger.debug(f"Manifest digest extracted from Location header: {digest}")
            return digest
        
        raise ValueError("Manifest digest not found in response headers.")