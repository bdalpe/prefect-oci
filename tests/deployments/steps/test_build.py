import os
import tarfile
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from prefect_oci.deployments.steps.build import (
    create_tar_archive,
    install_dependencies_for_archiving,
)


class TestCreateTarArchive:
    """Unit tests for create_tar_archive function."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_directory(self, temp_dir):
        """Create a sample directory structure for testing."""
        # Create directory structure
        (temp_dir / "subdir1").mkdir()
        (temp_dir / "subdir2").mkdir()

        # Create files
        (temp_dir / "file1.txt").write_text("Content 1")
        (temp_dir / "subdir1" / "file2.txt").write_text("Content 2")
        (temp_dir / "subdir2" / "file3.txt").write_text("Content 3")

        return temp_dir

    @pytest.mark.asyncio
    async def test_create_tar_archive_with_single_file(self, temp_dir):
        """Test creating an archive from a single file."""
        # Create a test file
        test_file = temp_dir / "test.txt"
        test_file.write_text("Test content")

        output_path = temp_dir / "archive.tar.gz"

        result = await create_tar_archive(
            sources="test.txt",  # Use relative path
            output_path=str(output_path),
            working_directory=str(temp_dir),
            ignore_file=None,
        )

        # Verify the archive was created
        assert Path(result["output_path"]).exists()
        assert result["output_path"] == str(output_path)

        # Verify archive contents
        with tarfile.open(output_path, "r:gz") as tar:
            members = tar.getmembers()
            assert len(members) == 1
            assert members[0].name == "test.txt"

    @pytest.mark.asyncio
    async def test_create_tar_archive_with_directory(self, sample_directory):
        """Test creating archive from a directory."""
        output_path = sample_directory / "archive.tar.gz"

        result = await create_tar_archive(
            sources=str(sample_directory / "subdir1"),
            output_path=str(output_path),
            working_directory=str(sample_directory),
            ignore_file=None,
        )

        # Verify the archive was created
        assert Path(result["output_path"]).exists()

        # Verify archive contents
        with tarfile.open(output_path, "r:gz") as tar:
            members = tar.getmembers()
            # Should contain the file from subdir1
            file_names = [m.name for m in members]
            assert any("file2.txt" in name for name in file_names)

    @pytest.mark.asyncio
    async def test_create_tar_archive_with_multiple_sources(self, sample_directory):
        """Test creating an archive from multiple sources."""
        output_path = sample_directory / "archive.tar.gz"

        result = await create_tar_archive(
            sources=[
                "file1.txt",  # Use relative paths
                "subdir1",
            ],
            output_path=str(output_path),
            working_directory=str(sample_directory),
            ignore_file=None,
        )

        # Verify the archive was created
        assert Path(result["output_path"]).exists()

        # Verify archive contents
        with tarfile.open(output_path, "r:gz") as tar:
            members = tar.getmembers()
            file_names = [m.name for m in members]
            # Should contain file1.txt and files from subdir1
            assert "file1.txt" in file_names
            assert any("file2.txt" in name for name in file_names)

    @pytest.mark.asyncio
    async def test_create_tar_archive_with_archive_root(self, temp_dir):
        """Test creating an archive with custom archive root."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("Test content")

        output_path = temp_dir / "archive.tar.gz"

        result = await create_tar_archive(
            sources="test.txt",  # Use relative path
            output_path=str(output_path),
            archive_root="custom/root",
            working_directory=str(temp_dir),
            ignore_file=None,
        )

        # Verify archive contents have the custom root
        with tarfile.open(output_path, "r:gz") as tar:
            members = tar.getmembers()
            assert len(members) == 1
            assert members[0].name == "custom/root/test.txt"

    @pytest.mark.asyncio
    async def test_create_tar_archive_with_ignore_file(self, sample_directory):
        """Test creating an archive with .prefectignore file."""
        # Create a .prefectignore file
        ignore_file = sample_directory / ".prefectignore"
        ignore_file.write_text("subdir2/*\n")

        output_path = sample_directory / "archive.tar.gz"

        result = await create_tar_archive(
            sources=str(sample_directory),
            output_path=str(output_path),
            working_directory=str(sample_directory),
            ignore_file=str(ignore_file),
        )

        # Verify the archive was created
        assert Path(result["output_path"]).exists()

        # Verify archive contents exclude ignored files
        with tarfile.open(output_path, "r:gz") as tar:
            members = tar.getmembers()
            file_names = [m.name for m in members]
            # Should not contain files from subdir2
            assert not any("file3.txt" in name for name in file_names)

    @pytest.mark.asyncio
    async def test_create_tar_archive_default_output_path(self, temp_dir):
        """Test creating an archive with default output path."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("Test content")

        result = await create_tar_archive(
            sources="test.txt",  # Use relative path
            working_directory=str(temp_dir),
            ignore_file=None,
        )

        # Verify an output path was created
        assert "output_path" in result
        assert Path(result["output_path"]).exists()

    @pytest.mark.asyncio
    async def test_create_tar_archive_deterministic(self, temp_dir):
        """Test that archives are deterministic (reproducible)."""
        test_file = temp_dir / "test.txt"
        test_file.write_text("Test content")

        output1 = temp_dir / "archive1.tar.gz"
        output2 = temp_dir / "archive2.tar.gz"

        # Create two archives with the same input
        await create_tar_archive(
            sources="test.txt",  # Use relative path
            output_path=str(output1),
            working_directory=str(temp_dir),
            ignore_file=None,
        )

        await create_tar_archive(
            sources="test.txt",  # Use relative path
            output_path=str(output2),
            working_directory=str(temp_dir),
            ignore_file=None,
        )

        # Verify that the archives have the same content (should be identical)
        with open(output1, "rb") as f1, open(output2, "rb") as f2:
            assert f1.read() == f2.read()


class TestInstallDependenciesForArchiving:
    """Unit tests for install_dependencies_for_archiving function."""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def requirements_file(self, temp_dir):
        """Create a sample requirements.txt file."""
        req_file = temp_dir / "requirements.txt"
        req_file.write_text("requests==2.31.0\n")
        return req_file

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    async def test_install_with_uv(
        self, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test installing dependencies with uv."""
        mock_which.return_value = "/usr/bin/uv"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
        )

        # Verify uv was used
        mock_which.assert_called_once_with("uv")
        mock_run_process.assert_called_once()

        # Check the command includes uv
        call_args = mock_run_process.call_args
        command = call_args[0][0]
        assert command[0] == "/usr/bin/uv"
        assert "pip" in command
        assert "install" in command
        assert str(requirements_file) in command
        assert str(target_dir) in command

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    @patch("prefect_oci.deployments.steps.build.sys")
    async def test_install_without_uv(
        self, mock_sys, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test installing dependencies without uv (fallback to pip)."""
        mock_which.return_value = None
        mock_sys.executable = "/usr/bin/python3"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
        )

        # Verify pip was used as fallback
        mock_which.assert_called_once_with("uv")
        mock_run_process.assert_called_once()

        # Check the command uses python -m pip
        call_args = mock_run_process.call_args
        command = call_args[0][0]
        assert command[0] == "/usr/bin/python3"
        assert command[1] == "-m"
        assert "pip" in command

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    async def test_install_with_platform(
        self, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test installing dependencies with platform specifier."""
        mock_which.return_value = "/usr/bin/uv"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
            platform="linux",
        )

        # Check the command includes platform
        call_args = mock_run_process.call_args
        command = call_args[0][0]
        assert "--python-platform" in command
        assert "linux" in command

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    async def test_install_with_additional_args(
        self, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test installing dependencies with additional pip args."""
        mock_which.return_value = "/usr/bin/uv"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
            additional_pip_args=["--no-cache-dir", "--upgrade"],
        )

        # Check the command includes additional args
        call_args = mock_run_process.call_args
        command = call_args[0][0]
        assert "--no-cache-dir" in command
        assert "--upgrade" in command

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    async def test_install_with_stream_output_disabled(
        self, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test installing dependencies with stream_output disabled."""
        mock_which.return_value = "/usr/bin/uv"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
            stream_output=False,
        )

        # Check that stream_output was passed correctly
        call_args = mock_run_process.call_args
        assert call_args[1]["stream_output"] == False

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    @patch("prefect_oci.deployments.steps.build.LoggerWriter")
    async def test_install_with_stream_output_enabled(
        self, mock_logger_writer, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test installing dependencies with stream_output enabled."""
        mock_which.return_value = "/usr/bin/uv"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
            stream_output=True,
        )

        # Check that LoggerWriter was called
        assert mock_logger_writer.call_count == 2

    @pytest.mark.asyncio
    @patch("prefect_oci.deployments.steps.build.run_process")
    @patch("prefect_oci.deployments.steps.build.shutil.which")
    async def test_install_dependencies_returns_dict(
        self, mock_which, mock_run_process, temp_dir, requirements_file
    ):
        """Test that install_dependencies_for_archiving returns the expected dictionary."""
        mock_which.return_value = "/usr/bin/uv"
        mock_run_process.return_value = AsyncMock()

        target_dir = temp_dir / "deps"
        target_dir.mkdir()

        result = await install_dependencies_for_archiving(
            requirements_file=str(requirements_file),
            target_directory=str(target_dir),
        )

        assert isinstance(result, dict)
        assert result["target_directory"] == str(target_dir)
        assert result["requirements_file"] == str(requirements_file)

