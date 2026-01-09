import logging
import base64
from typing import Optional, Tuple, Dict, Any

logger = logging.getLogger(__name__)


def _get_ecr_token(credentials: dict[str, Any]) -> Tuple[str, str]:
    """
    Retrieve ECR authorization token using AwsCredentials.
    
    :param credentials: dictionary of AWS credentials
    :return: Tuple of (username, password)
    """
    try:
        from prefect_aws import AwsCredentials
    except ImportError as e:
        raise ImportError(
            "prefect-aws is required for ECR authentication. Please install it with `pip install prefect-aws`."
        ) from e

    try:
        # Hydrate AwsCredentials object from dict
        aws_credentials = AwsCredentials.model_validate(credentials)
        
        logger.debug("Getting ECR client using AwsCredentials (region: %s)", aws_credentials.region_name)
        # Use instantiated object to get client
        client = aws_credentials.get_client("ecr")
        response = client.get_authorization_token()
        auth_data = response.get('authorizationData', [])
        
        if not auth_data:
            raise ValueError("No authorization data returned from ECR")
        
        token_b64 = auth_data[0]['authorizationToken']
        # Token is base64 encoded "AWS:password"
        token_decoded = base64.b64decode(token_b64).decode('utf-8')
        username, password = token_decoded.split(':', 1)
        
        return username, password
    except Exception as e:
        logger.error("Failed to retrieve ECR auth token: %s", e)
        raise ValueError(f"Failed to retrieve ECR auth token: {e}") from e


def resolve_credentials(
    credentials: dict[str, Any], 
    registry_url: str
) -> Tuple[Optional[str], Optional[str], Optional[str], str]:
    """
    Resolve credentials from a dictionary of credentials.
    
    :param credentials: Dictionary of credentials (DockerRegistryCredentials or AwsCredentials)
    :param registry_url: The registry URL for validation - used if registry_url is not in the block
    :return: Tuple of (username, password, registry_url, auth_backend)
    """
    if credentials is None:
        return None, None, None, "token"

    # Check for AwsCredentials keys
    # Check for known AWS keys including profile/role/access_key/region
    # We check for a subset of keys that would indicate this is intended as an AwsCredentials block
    aws_keys = {
        'aws_access_key_id', 
        'profile_name', 
        'region_name', 
        'assume_role_arn'
    }
    if any(key in credentials for key in aws_keys):
        logger.debug("Detected AwsCredentials compatible dictionary")
        # Use region from credentials if available
        username, password = _get_ecr_token(credentials)
        return username, password, registry_url, "basic"

    # Check for DockerRegistryCredentials (username/password)
    # This is the fallback for generic registry credentials
    if 'username' in credentials and 'password' in credentials:
        logger.debug("Detected DockerRegistryCredentials compatible dictionary")
        username = credentials['username']
        # Handle SecretStr if present (though dict usually has raw values if dumped from block)
        password = credentials['password']
        if hasattr(password, 'get_secret_value'):
             password = password.get_secret_value()
             
        registry_url_from_block = credentials.get('registry_url', registry_url)
        return username, password, registry_url_from_block, "token"
    
    # Unrecognized format
    raise ValueError(
        "Unsupported credentials format. Expected dictionary with either "
        "AwsCredentials fields (access_key/profile/role) or "
        "DockerRegistryCredentials fields (username/password)."
    )
