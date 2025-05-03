# src/phase2_parsing/Vector_storer/vector_storer.py
"""
VectorStorer: upsert FinLensNode embeddings and payloads into a vector database.
"""
from typing import List, Dict
from src.phase2_parsing.node_builders.node_builder import FinLensNode


class VectorStorer:

    def __init__(self, vector_client):
        """
        vector_client: client with methods upsert(points: List[Dict])
        Each point dict should have keys: id, vector, payload
        """
        self.client = vector_client

    def upsert_node(self, node: FinLensNode, vector: List[float]):
        point = {'id': node.node_id, 'vector': vector, 'payload': node.dict()}
        # single upsert
        self.client.upsert([point])

    def upsert_nodes(self, nodes: List[FinLensNode],
                     vectors: List[List[float]]):
        points = []
        for node, vec in zip(nodes, vectors):
            points.append({
                'id': node.node_id,
                'vector': vec,
                'payload': node.dict()
            })
        self.client.upsert(points)
