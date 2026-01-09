import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from prefect_oci.deployments.steps.push import push_oci_image, PlatformManifest


class TestPlatformManifest:
    """Unit tests for PlatformManifest model."""

    def test_platform_manifest_validation(self):
        """Test that PlatformManifest validates correctly."""
        data = {
            "platform": {
                "architecture": "amd64",
                "os": "linux",
            },
            "layers": ["/path/to/layer1.tar.gz", "/path/to/layer2.tar.gz"],
        }

        manifest = PlatformManifest.model_validate(data)

        assert manifest.layers == data["layers"]
        assert manifest.platform.architecture == "amd64"
        assert manifest.platform.os == "linux"

    def test_platform_manifest_invalid(self):
        """Test that PlatformManifest rejects invalid data."""
        data = {
            "platform": {},
            # Missing layers
        }

        with pytest.raises(Exception):  # Pydantic validation error
            PlatformManifest.model_validate(data)


class TestPushOCIImage:
    """Unit tests for push_oci_image function."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_layers(self, temp_dir):
        """Create sample layer files."""
        import gzip
        import tarfile

        layer1 = temp_dir / "layer1.tar.gz"
        layer2 = temp_dir / "layer2.tar.gz"

        # Create actual gzipped tar files
        for layer_path, content in [(layer1, b"Layer 1 content"), (layer2, b"Layer 2 content")]:
            with gzip.open(layer_path, 'wb') as gz:
                with tarfile.open(fileobj=gz, mode='w') as tar:
                    # Add a simple text file to the tar
                    import io
                    data = io.BytesIO(content)
                    info = tarfile.TarInfo(name="content.txt")
                    info.size = len(content)
                    tar.addfile(info, data)

        return [str(layer1), str(layer2)]

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    async def test_push_simple_manifest(
        self,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
    ):
        """Test pushing a simple OCI image with layers as a list of strings."""
        # Setup mocks
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "1234"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = (
            "sha256:abc123"
        )

        manifest_data = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"digest": "sha256:config123"},
            "layers": [{"digest": "sha256:layer1"}, {"digest": "sha256:layer2"}],
        }
        mock_client.get_manifest.return_value = manifest_data

        mock_index_manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "digest": "sha256:abc123",
                    "size": 1234,
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                }
            ],
        }
        mock_create_index.return_value = mock_index_manifest

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = (
            "sha256:index123"
        )

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "localhost:5000/test-image"
        mock_container_instance.__str__ = lambda self: "localhost:5000/test-image:latest"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        # Call the function
        result = await push_oci_image(
            name="localhost:5000/test-image",
            tag="latest",
            layers=sample_layers,
        )

        # Verify the result
        assert result["name"] == "localhost:5000/test-image"
        assert result["tag"] == "latest"
        assert "digest" in result
        assert "image" in result

        # Verify Registry was instantiated
        mock_registry.assert_called_once_with()

        # Verify push was called
        mock_client.push.assert_called_once()

        # Verify image index was uploaded
        mock_client.upload_image_index.assert_called_once()

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    async def test_push_with_custom_client_kwargs(
        self,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
    ):
        """Test pushing with custom client kwargs."""
        # Setup mocks
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "1234"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = (
            "sha256:abc123"
        )

        manifest_data = {
            "schemaVersion": 2,
            "layers": [],
        }
        mock_client.get_manifest.return_value = manifest_data

        mock_create_index.return_value = {"manifests": []}

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "registry.example.com/test"
        mock_container_instance.__str__ = lambda self: "registry.example.com/test:v1"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        custom_kwargs = {"insecure": True, "username": "test", "password": "secret"}

        # Call the function
        result = await push_oci_image(
            name="registry.example.com/test",
            tag="v1",
            layers=sample_layers,
            client_kwargs=custom_kwargs,
        )

        # Verify Registry was instantiated with custom kwargs
        mock_registry.assert_called_once_with(**custom_kwargs)

        # Verify basic result structure
        assert result["name"] == "registry.example.com/test"
        assert result["tag"] == "v1"

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.deployments.steps.push.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    @patch("prefect_oci.deployments.steps.push.diff_id_from_tar_gz")
    async def test_push_multiplatform_image(
        self,
        mock_diff_id,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
    ):
        """Test pushing a multi-platform OCI image."""
        # Setup mocks
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_diff_id.return_value = "abc123def456"

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "2000"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.side_effect = [
            "sha256:linux_amd64",
            "sha256:linux_arm64",
            "sha256:multiplatform_index",  # For the final image index upload
        ]

        manifest_data_amd64 = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"digest": "sha256:config_amd64"},
            "layers": [{"digest": "sha256:layer_amd64"}],
        }

        manifest_data_arm64 = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"digest": "sha256:config_arm64"},
            "layers": [{"digest": "sha256:layer_arm64"}],
        }

        mock_client.get_manifest.side_effect = [manifest_data_amd64, manifest_data_arm64]

        mock_index_manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "manifests": [
                {
                    "digest": "sha256:linux_amd64",
                    "platform": {"os": "linux", "architecture": "amd64"},
                },
                {
                    "digest": "sha256:linux_arm64",
                    "platform": {"os": "linux", "architecture": "arm64"},
                },
            ],
        }
        mock_create_index.return_value = mock_index_manifest

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "registry/multiarch"
        mock_container_instance.__str__ = lambda self: "registry/multiarch:latest"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        # Create multi-platform layers
        platform_layers = [
            {
                "platform": {"os": "linux", "architecture": "amd64"},
                "layers": [sample_layers[0]],
            },
            {
                "platform": {"os": "linux", "architecture": "arm64"},
                "layers": [sample_layers[1]],
            },
        ]

        # Call the function
        result = await push_oci_image(
            name="registry/multiarch",
            tag="latest",
            layers=platform_layers,
        )

        # Verify the result
        assert result["name"] == "registry/multiarch"
        assert result["tag"] == "latest"
        assert "digest" in result

        # Verify push was called twice (once per platform)
        assert mock_client.push.call_count == 2

        # Verify image index was created
        mock_create_index.assert_called_once()

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    async def test_push_creates_container_correctly(
        self, mock_container, mock_registry, sample_layers
    ):
        """Test that Container is created with the correct name and tag."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        # Setup minimal mocks to avoid errors
        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "100"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = (
            "sha256:test"
        )
        mock_client.get_manifest.return_value = {"schemaVersion": 2, "layers": []}

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "test-registry/image"
        mock_container_instance.__str__ = lambda self: "test-registry/image:v2.0"
        mock_container.return_value = mock_container_instance

        with patch("prefect_oci.provider.image.create_oci_image_index_manifest"):
            with patch("oras.provider.temporary_empty_config"):
                with patch("prefect_oci.provider.oci.EmptyManifestConfig"):
                    await push_oci_image(
                        name="test-registry/image",
                        tag="v2.0",
                        layers=sample_layers,
                    )

        # Verify Container was created with the correct string
        mock_container.assert_called_once_with("test-registry/image:v2.0")

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    async def test_push_uses_correct_media_types(
        self,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
    ):
        """Test that correct media types are used when pushing."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "500"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = (
            "sha256:digest"
        )
        mock_client.get_manifest.return_value = {"schemaVersion": 2, "layers": []}

        mock_create_index.return_value = {"manifests": []}

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "test/image"
        mock_container_instance.__str__ = lambda self: "test/image:latest"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        await push_oci_image(
            name="test/image",
            tag="latest",
            layers=sample_layers,
        )

        # Verify push was called with correct media types
        push_call_args = mock_client.push.call_args
        files = push_call_args[1]["files"]

        # Check that all files have the correct media type
        for file in files:
            assert file.endswith(":application/vnd.oci.image.layer.v1.tar+gzip")


class TestPushOCIImageCredentials:
    """Unit tests for push_oci_image with credentials."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_layers(self, temp_dir):
        """Create sample layer files."""
        import gzip
        import tarfile

        layer1 = temp_dir / "layer1.tar.gz"
        with gzip.open(layer1, 'wb') as gz:
            with tarfile.open(fileobj=gz, mode='w') as tar:
                import io
                data = io.BytesIO(b"Layer 1 content")
                info = tarfile.TarInfo(name="content.txt")
                info.size = len(b"Layer 1 content")
                tar.addfile(info, data)

        return [str(layer1)]

    @pytest.fixture
    def mock_docker_credentials(self):
        """Create a mock DockerRegistryCredentials block."""
        mock_creds = MagicMock()
        mock_creds.username = "testuser"
        mock_password = MagicMock()
        mock_password.get_secret_value.return_value = "testpass"
        mock_creds.password = mock_password
        mock_creds.registry_url = "my-registry.com"
        return mock_creds

    @pytest.fixture
    def mock_aws_credentials(self):
        """Create a mock AwsCredentials block."""
        mock_creds = MagicMock()
        mock_creds.aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"
        mock_secret = MagicMock()
        mock_secret.get_secret_value.return_value = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        mock_creds.aws_secret_access_key = mock_secret
        mock_creds.region_name = "us-east-1"
        return mock_creds

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    @patch("prefect_oci.provider.auth.resolve_credentials")
    async def test_push_with_docker_credentials(
        self,
        mock_resolve_creds,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
        mock_docker_credentials,
    ):
        """Test pushing with DockerRegistryCredentials block."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "1234"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = "sha256:abc123"
        mock_client.get_manifest.return_value = {"schemaVersion": 2, "layers": []}
        mock_create_index.return_value = {"manifests": []}

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "my-registry.com/test-image"
        mock_container_instance.registry = "my-registry.com"
        mock_container_instance.__str__ = lambda self: "my-registry.com/test-image:latest"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        # Configure resolve_credentials
        mock_resolve_creds.return_value = ("testuser", "testpass", "my-registry.com", "token")

        result = await push_oci_image(
            name="my-registry.com/test-image",
            tag="latest",
            layers=sample_layers,
            credentials=mock_docker_credentials,
        )

        # Verify resolve_credentials was called correctly
        mock_resolve_creds.assert_called_once_with(
            mock_docker_credentials,
            "my-registry.com",
        )

        # Verify Registry was instantiated with auth_backend
        mock_registry.assert_called_once_with(auth_backend="token")
        
        # Verify call to login
        mock_client.login.assert_called_once_with(
            username="testuser",
            password="testpass",
            hostname="my-registry.com"
        )

        assert result["name"] == "my-registry.com/test-image"

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    @patch("prefect_oci.provider.auth.resolve_credentials")
    @patch.dict("os.environ", {}, clear=False)
    async def test_push_with_aws_credentials_ecr(
        self,
        mock_resolve_creds,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
        mock_aws_credentials,
    ):
        """Test pushing to ECR with AwsCredentials block."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "1234"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = "sha256:abc123"
        mock_client.get_manifest.return_value = {"schemaVersion": 2, "layers": []}
        mock_create_index.return_value = {"manifests": []}

        mock_container_instance = MagicMock()
        ecr_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image"
        mock_container_instance.api_prefix = ecr_url
        mock_container_instance.registry = "123456789012.dkr.ecr.us-east-1.amazonaws.com"
        mock_container_instance.__str__ = lambda self: f"{ecr_url}:latest"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        # Configure resolve_credentials
        # Assume it fetches token and returns username/password/registry/backend
        mock_resolve_creds.return_value = ("AWS", "secret-token", "123456789012.dkr.ecr.us-east-1.amazonaws.com", "token")

        result = await push_oci_image(
            name=ecr_url,
            tag="latest",
            layers=sample_layers,
            credentials=mock_aws_credentials,
        )

        # Verify resolve_credentials was called
        mock_resolve_creds.assert_called_once_with(
            mock_aws_credentials,
            "123456789012.dkr.ecr.us-east-1.amazonaws.com",
        )

        # Verify Registry instantiated
        mock_registry.assert_called_once_with(auth_backend="token")
        
        # Verify call to login
        mock_client.login.assert_called_once_with(
            username="AWS",
            password="secret-token",
            hostname="123456789012.dkr.ecr.us-east-1.amazonaws.com"
        )

        assert result["name"] == ecr_url

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    @patch("prefect_oci.provider.auth.resolve_credentials")
    async def test_push_with_credentials_and_client_kwargs(
        self,
        mock_resolve_creds,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
        mock_docker_credentials,
    ):
        """Test that credentials and client_kwargs are merged correctly."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "1234"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = "sha256:abc123"
        mock_client.get_manifest.return_value = {"schemaVersion": 2, "layers": []}
        mock_create_index.return_value = {"manifests": []}

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "my-registry.com/test"
        mock_container_instance.registry = "my-registry.com"
        mock_container_instance.__str__ = lambda self: "my-registry.com/test:v1"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        existing_kwargs = {"insecure": True}
        # Credentials resolved
        mock_resolve_creds.return_value = ("testuser", "testpass", "my-registry.com", "token")

        result = await push_oci_image(
            name="my-registry.com/test",
            tag="v1",
            layers=sample_layers,
            credentials=mock_docker_credentials,
            client_kwargs=existing_kwargs,
        )

        # Verify resolve_credentials call
        mock_resolve_creds.assert_called_once_with(
            mock_docker_credentials,
            "my-registry.com",
        )

        # Verify Registry instantiated with client_kwargs and auth_backend
        mock_registry.assert_called_once_with(auth_backend="token", **existing_kwargs)
        
        # Verify login was called
        mock_client.login.assert_called_once_with(
            username="testuser",
            password="testpass",
            hostname="my-registry.com"
        )
        
        assert result["name"] == "my-registry.com/test"

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    @patch("prefect_oci.provider.container.Container")
    @patch("prefect_oci.provider.image.create_oci_image_index_manifest")
    @patch("oras.provider.temporary_empty_config")
    @patch("prefect_oci.provider.oci.EmptyManifestConfig")
    @patch("prefect_oci.provider.auth.resolve_credentials")
    async def test_push_without_credentials(
        self,
        mock_resolve_creds,
        mock_empty_config,
        mock_temp_config,
        mock_create_index,
        mock_container,
        mock_registry,
        sample_layers,
    ):
        """Test that push works without credentials."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        mock_response = MagicMock()
        mock_response.headers = {"Content-Length": "1234"}
        mock_client.push.return_value = mock_response
        mock_client.extract_manifest_digest_from_upload_response.return_value = "sha256:abc123"
        mock_client.get_manifest.return_value = {"schemaVersion": 2, "layers": []}
        mock_create_index.return_value = {"manifests": []}

        mock_container_instance = MagicMock()
        mock_container_instance.api_prefix = "public-registry.com/image"
        mock_container_instance.registry = "public-registry.com"
        mock_container_instance.__str__ = lambda self: "public-registry.com/image:latest"
        mock_container.return_value = mock_container_instance
        mock_container.with_new_digest.return_value = mock_container_instance

        mock_index_response = MagicMock()
        mock_client.upload_image_index.return_value = mock_index_response

        # Configure resolve_credentials to safe defaults
        mock_resolve_creds.return_value = (None, None, None, "token")

        result = await push_oci_image(
            name="public-registry.com/image",
            tag="latest",
            layers=sample_layers,
        )

        # Verify resolve_credentials called with None
        mock_resolve_creds.assert_called_once_with(None, "public-registry.com")

        # Verify Registry instantiated
        mock_registry.assert_called_once_with()
        
        # Verify login NOT called
        mock_client.login.assert_not_called()

        assert result["name"] == "public-registry.com/image"
