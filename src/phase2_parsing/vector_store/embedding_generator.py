# src/phase2_parsing/vector_store/embedding_generator.py
"""
EmbeddingGenerator: create embeddings for FinLensNode objects.
TEXT/HEADING nodes embed their text_content or title;
TABLE nodes embed only the title (caption) or section header.
"""
from typing import List
from src.phase2_parsing.node_builders.ToC_node_builder import FinLensNode


class EmbeddingGenerator:

    def __init__(self, embed_client):
        """
        embed_client: any object with method embed(text: str) -> List[float]
        """
        self.client = embed_client

    def embed_node(self, node: FinLensNode) -> List[float]:
        # Choose embedding input by node type
        if node.node_type == "TABLE":
            text = node.title or ''
        else:
            text = node.text_content or node.title or ''
        # Call underlying embedding client
        vector = self.client.embed(text)
        return vector

    def embed_nodes(self, nodes: List[FinLensNode]) -> List[List[float]]:
        # Batch embed if client supports batch, else loop
        try:
            texts = []
            for node in nodes:
                if node.node_type == "TABLE":
                    texts.append(node.title or '')
                else:
                    texts.append(node.text_content or node.title or '')
            # assume client supports embed_batch
            return self.client.embed_batch(texts)
        except AttributeError:
            # fallback to individual calls
            return [self.embed_node(n) for n in nodes]
