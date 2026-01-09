import copy

from prefect_oci.provider.defaults import default_image_index_media_type

EmptyImageIndex: dict = {
    "schemaVersion": 2,
    "mediaType": default_image_index_media_type,
    "manifests": [],
    "annotations": {},
}

def NewImageIndex() -> dict:
    """
    Get an empty index config.
    """
    return copy.deepcopy(EmptyImageIndex)


def create_oci_image_index_manifest(manifests: list[dict]) -> dict:
    """
    Create a simple OCI image manifest with the given manifests.

    :param manifests: A list of manifests to include in the image.
    
    :return: An OCI image manifest dictionary.
    """
    image_index = NewImageIndex()

    # Ensure we remove layers and config from the manifest (if they exist) before adding to the index
    for mfst in manifests:
        m = {key: value for key, value in mfst.items() if key not in {"schemaVersion", "layers", "config"}}
        image_index['manifests'].append(m)

    return image_index