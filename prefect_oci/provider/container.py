import copy
from typing import Optional

import oras.defaults
from oras.container import Container as ORASContainer


class Container(ORASContainer):
    def __init__(self, name: str, registry: Optional[str] = None):
        """
        Parse a container name and easily get urls for registry interactions.

        :param name: the full name of the container to parse (with any components)
        :type name: str
        :param registry: a custom registry name, if not provided with URI
        :type registry: str
        """
        self.registry = registry or oras.defaults.registry.default_v2_registry["host"]

        # Registry is the name takes precendence
        self.parse(name)
    
    @classmethod
    def with_new_digest(cls, original: "Container", digest: str) -> "Container":
        """
        Create a new Container instance based on an existing one but with a new digest.

        :param original: The original Container instance.
        :param digest: The new digest to use.
        :return: A new Container instance with the updated digest.
        """
        container = copy.deepcopy(original)
        
        # remove tag if present
        container.tag = None
        
        container.digest = digest
        
        return container