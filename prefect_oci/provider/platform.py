import platform
import re
from typing import Optional

from pydantic import BaseModel, Field

# The value takes the form of `os/arch` or `os/arch/variant`.
# https://docs.docker.com/reference/cli/docker/buildx/build/#platform
platform_regex = re.compile(
    "(?P<os>[^/]+)"
    "(?:/(?P<architecture>[^/]+)"
    "(?:/(?P<variant>[^/]+))?)?"
    "$"
)


class Platform(BaseModel):
    os: str = Field(
        ...,
        description="Operating system",
        examples=["linux", "windows"]
    )
    
    architecture: str = Field(
        ...,
        description="CPU architecture",
        examples=["amd64", "arm64"]
    )
    
    variant: Optional[str] = Field(
        None,
        description="CPU variant",
        examples=["v7", "v8"]
    )
            
    @classmethod
    def from_str(cls, platform_str: str) -> "Platform":
        match = platform_regex.match(platform_str)

        if not match:
            raise ValueError(f"Invalid platform string: {platform_str}")
        
        data = {
            "os": match.group("os"), 
            "architecture": match.group("architecture"), 
            "variant": match.group("variant")
        }

        return Platform(**data)

    def is_match(self, other: dict) -> bool:
        """
        Check if this platform matches another platform dict.

        :param other: the other platform dict to compare against
        :return: True if they match, False otherwise
        """
        return all([
            other.get("os") is None or self.os == other.get("os"),
            other.get("architecture") is None or self.architecture == other.get("architecture"),
            other.get("variant") is None or self.variant == other.get("variant"),
        ])

    def to_dict(self):
        result = {
            "os": self.os,
            "architecture": self.architecture,
        }

        if self.variant:
            result.update(
                {"variant": self.variant}
            )

        return result

    @classmethod
    def detect_system(cls) -> "Platform":
        """
        Detect the current system's platform.
        """
        os_name = platform.system().lower()
        arch = platform.machine().lower()

        os_map = {
            "linux": "linux",
            "darwin": "linux",  # Docker on macOS runs Linux containers
            "windows": "windows",
        }
        os = next((v for k, v in os_map.items() if os_name.startswith(k)), None)
        if not os:
            raise RuntimeError(f"Unsupported OS: {os_name}")

        arch_map = {
            "x86_64": "amd64",
            "amd64": "amd64",
            "aarch64": "arm64",
            "arm64": "arm64",
        }
        architecture = arch_map.get(arch, "arm" if arch.startswith("arm") else None)
        if not architecture:
            raise RuntimeError(f"Unsupported architecture: {arch}")

        return Platform(os=os, architecture=architecture)