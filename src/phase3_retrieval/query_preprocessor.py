# src/phase3_retrieval/query_preprocessor.py
"""
QueryPreprocessor: analyze user query and produce retrieval filters.
"""
import re
from typing import Dict, Any


class QueryPreprocessor:

    def preprocess(self, query: str) -> Dict[str, Any]:
        """
        Very simple rule-based: extract 'Item N' or 'Item N[A]' target.
        Returns dict with optional 'section_id' and 'keywords'.
        """
        result: Dict[str, Any] = {'keywords': query}
        m = re.search(r"ITEM\s+(\d+[A-Z]?)", query, re.IGNORECASE)
        if m:
            sec = m.group(1).lower()
            result[
                'section_id'] = f"h{2 if len(sec)==1 or sec.isdigit() else 3}_item_{sec}"
        return result


# src/phase3_retrieval/retrieval_service.py
"""
RetrievalService: given preprocessed plan, fetch nodes via vector DB client.
"""
from typing import List, Dict, Any
from src.phase2_parsing.node_builder import FinLensNode


class RetrievalService:

    def __init__(self, vector_client, threshold: float = 0.7):
        self.client = vector_client
        self.threshold = threshold

    def retrieve(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        plan may include 'section_id', 'keywords'.
        We embed keywords, then query vector DB with metadata filter.
        Returns list of payloads.
        """
        # 1. embed query
        query_vec = self.client.embed(plan['keywords'])
        # 2. apply filter
        filter = {}
        if 'section_id' in plan:
            filter['section_id'] = plan['section_id']
        # 3. search
        hits = self.client.search(vector=query_vec,
                                  filter=filter,
                                  top_k=10,
                                  min_score=self.threshold)
        # 4. return payloads
        return [hit['payload'] for hit in hits]
