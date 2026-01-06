import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from prefect_oci.deployments.steps.pull import pull_oci_image


class TestPullOCIImage:
    """Unit tests for pull_oci_image function."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_basic(self, mock_registry, temp_dir):
        """Test basic pulling of an OCI image."""
        # Setup mocks
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        # Mock the pull method to return a list of files
        expected_files = [
            str(temp_dir / "layer1.tar.gz"),
            str(temp_dir / "layer2.tar.gz"),
        ]
        mock_client.pull.return_value = expected_files

        # Call the function
        result = await pull_oci_image(
            name="test-registry/test-image",
            tag="latest",
            path=str(temp_dir),
        )

        # Verify the result
        assert result["files"] == expected_files
        assert result["path"] == str(temp_dir)

        # Verify Registry was instantiated
        mock_registry.assert_called_once_with()

        # Verify pull was called with correct arguments
        mock_client.pull.assert_called_once_with(
            "test-registry/test-image:latest",
            outdir=str(temp_dir),
        )

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_with_default_path(self, mock_registry):
        """Test pulling with default path (current directory)."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        expected_files = ["layer1.tar.gz"]
        mock_client.pull.return_value = expected_files

        # Call the function without specifying path
        result = await pull_oci_image(
            name="registry/image",
            tag="v1.0",
        )

        # Verify the result uses current working directory
        assert result["files"] == expected_files
        assert result["path"] == os.getcwd()

        # Verify pull was called with cwd
        mock_client.pull.assert_called_once_with(
            "registry/image:v1.0",
            outdir=os.getcwd(),
        )

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_with_custom_client_kwargs(self, mock_registry, temp_dir):
        """Test pulling with custom client kwargs."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        expected_files = ["layer.tar.gz"]
        mock_client.pull.return_value = expected_files

        custom_kwargs = {
            "insecure": True,
            "username": "testuser",
            "password": "testpass",
        }

        # Call the function with custom kwargs
        result = await pull_oci_image(
            name="private.registry.com/secure-image",
            tag="secure-tag",
            path=str(temp_dir),
            client_kwargs=custom_kwargs,
        )

        # Verify Registry was instantiated with custom kwargs
        mock_registry.assert_called_once_with(**custom_kwargs)

        # Verify result
        assert result["files"] == expected_files
        assert result["path"] == str(temp_dir)

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_constructs_correct_image_reference(
        self, mock_registry, temp_dir
    ):
        """Test that image reference is constructed correctly."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client
        mock_client.pull.return_value = []

        # Test various name and tag combinations
        test_cases = [
            ("localhost:5000/image", "latest", "localhost:5000/image:latest"),
            ("registry.io/org/image", "v1.2.3", "registry.io/org/image:v1.2.3"),
            ("simple-image", "dev", "simple-image:dev"),
        ]

        for name, tag, expected_ref in test_cases:
            mock_client.reset_mock()

            await pull_oci_image(
                name=name,
                tag=tag,
                path=str(temp_dir),
            )

            # Verify pull was called with the correct image reference
            mock_client.pull.assert_called_once()
            call_args = mock_client.pull.call_args
            assert call_args[0][0] == expected_ref

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_returns_empty_files_list(self, mock_registry, temp_dir):
        """Test pulling when no files are returned."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        # Mock pull to return empty list
        mock_client.pull.return_value = []

        result = await pull_oci_image(
            name="empty/image",
            tag="latest",
            path=str(temp_dir),
        )

        # Verify the result has empty files list
        assert result["files"] == []
        assert result["path"] == str(temp_dir)

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_with_none_client_kwargs(self, mock_registry, temp_dir):
        """Test pulling when client_kwargs is None."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client
        mock_client.pull.return_value = ["file.tar.gz"]

        # Call with explicit None for client_kwargs
        result = await pull_oci_image(
            name="test/image",
            tag="latest",
            path=str(temp_dir),
            client_kwargs=None,
        )

        # Verify Registry was instantiated with an empty dict
        mock_registry.assert_called_once_with()

        # Verify result
        assert "files" in result
        assert "path" in result

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_multiple_layers(self, mock_registry, temp_dir):
        """Test pulling an image with multiple layers."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        # Mock pulling multiple layers
        expected_files = [
            str(temp_dir / "layer1.tar.gz"),
            str(temp_dir / "layer2.tar.gz"),
            str(temp_dir / "layer3.tar.gz"),
            str(temp_dir / "config.json"),
        ]
        mock_client.pull.return_value = expected_files

        result = await pull_oci_image(
            name="multi-layer/image",
            tag="latest",
            path=str(temp_dir),
        )

        # Verify all files are returned
        assert len(result["files"]) == 4
        assert result["files"] == expected_files

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_with_digest_tag(self, mock_registry, temp_dir):
        """Test pulling with a digest as the tag."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client
        mock_client.pull.return_value = ["layer.tar.gz"]

        # Use a digest instead of a regular tag
        digest_tag = "sha256:1234567890abcdef"

        result = await pull_oci_image(
            name="registry/image",
            tag=digest_tag,
            path=str(temp_dir),
        )

        # Verify pull was called with digest
        mock_client.pull.assert_called_once()
        call_args = mock_client.pull.call_args
        assert digest_tag in call_args[0][0]


class TestPullOCIImageIntegration:
    """Integration-style tests for pull_oci_image."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_creates_files_in_target_directory(
        self, mock_registry, temp_dir
    ):
        """Test that pull operation would create files in the target directory."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client

        # Create actual files in temp_dir to simulate what pull would do
        layer_file = temp_dir / "layer.tar.gz"
        layer_file.write_bytes(b"mock layer content")

        mock_client.pull.return_value = [str(layer_file)]

        result = await pull_oci_image(
            name="test/image",
            tag="latest",
            path=str(temp_dir),
        )

        # Verify the file exists
        assert layer_file.exists()
        assert str(layer_file) in result["files"]

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_with_relative_path(self, mock_registry, temp_dir):
        """Test pulling with a relative path."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client
        mock_client.pull.return_value = []

        # Create a subdirectory
        subdir = temp_dir / "subdir"
        subdir.mkdir()

        # Use relative path
        relative_path = "subdir"

        # Change to temp_dir to make a relative path work
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)

            result = await pull_oci_image(
                name="test/image",
                tag="latest",
                path=relative_path,
            )

            # Verify pull was called with the relative path
            mock_client.pull.assert_called_once()
            call_args = mock_client.pull.call_args
            assert call_args[1]["outdir"] == relative_path

        finally:
            os.chdir(original_cwd)

    @pytest.mark.asyncio
    @patch("prefect_oci.provider.registry.Registry")
    async def test_pull_handles_special_characters_in_names(
        self, mock_registry, temp_dir
    ):
        """Test pulling images with special characters in names."""
        mock_client = MagicMock()
        mock_registry.return_value = mock_client
        mock_client.pull.return_value = []

        # Test with special characters commonly used in registry names
        special_names = [
            "registry.io/org/image-name",
            "localhost:5000/test_image",
            "gcr.io/project-123/app",
            "docker.io/library/alpine",
        ]

        for name in special_names:
            mock_client.reset_mock()

            await pull_oci_image(
                name=name,
                tag="latest",
                path=str(temp_dir),
            )

            # Verify pull was called
            mock_client.pull.assert_called_once()
            call_args = mock_client.pull.call_args
            # Verify the name is in the constructed reference
            assert name in call_args[0][0]
