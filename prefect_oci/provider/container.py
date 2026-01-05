import copy

from oras.container import Container as ORASContainer


class Container(ORASContainer):
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