from oras.schemas import schema_url, manifestProperties

imageIndexManifestProperties = {
    **manifestProperties,
    "platform": {
        "type": "object",
        "properties": {
            "architecture": {"type": "string"},
            "os": {"type": "string"},
            "os.version": {"type": "string"},
            "os.features": {"type": "array", "items": {"type": "string"}},
            "variant": {"type": "string"},
            "features": {"type": "array", "items": {"type": "string"}},
        },
        "required": [
            "architecture",
            "os",
        ]
    }
}

imageIndexProperties = {
    "schemaVersion": {"type": "number"},
    "mediaType": {"type": "string"},
    "artifactType": {"type": ["null", "string"]},
    "subject": {"type": ["null", "object"]},
    "annotations": {"type": ["object", "null", "array"]},
    "manifests": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": imageIndexManifestProperties
        }
    },
}

image_index = {
    "$schema": schema_url,
    "title": "Index Schema",
    "type": "object",
    "required": [
        "schemaVersion",
        "manifests",
    ],
    "properties": imageIndexProperties,
    "additionalProperties": True,
}
