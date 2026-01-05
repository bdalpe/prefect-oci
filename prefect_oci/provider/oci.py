from oras.oci import ManifestConfig

import prefect_oci.provider.defaults


def EmptyManifestConfig() -> dict:
    config, _ = ManifestConfig(media_type=prefect_oci.provider.defaults.default_empty_json_media_type)
    
    """
    Embed an empty JSON object as the config data.
    
    This reduces the need for an extra HTTP request to the registry     
    which can have performance implications.
    
    see:
    * https://github.com/opencontainers/image-spec/blob/main/descriptor.md#embedded-content
    * https://github.com/opencontainers/image-spec/blob/main/manifest.md#guidance-for-an-empty-descriptor
    """
    config['data'] = prefect_oci.provider.defaults.default_empty_json_object_base64  # base64 encoded '{}'

    return config