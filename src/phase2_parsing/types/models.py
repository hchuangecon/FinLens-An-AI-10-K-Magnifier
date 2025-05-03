# src/phase2_parsing/types/models.py
# Add this class definition to your existing models file.

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any, Union


class SecParserMetadata(BaseModel):
    """
    Metadata specifically derived from the sec-parser process for a FinLensNode.
    """
    # Allow arbitrary types for potentially storing non-serializable objects temporarily,
    # though it's recommended to store only serializable data here eventually.
    # model_config = ConfigDict(arbitrary_types_allowed=True)

    source_element_type: Optional[str] = Field(
        None,
        description=
        "The class name of the originating sec-parser semantic element.")
    source_element_hash: Optional[int] = Field(
        None,
        description=
        "Hash of the originating sec-parser element for potential traceability (None for synthetic)."
    )
    tag_name: Optional[str] = Field(
        None,
        description=
        "The HTML tag name of the originating element (e.g., 'p', 'div', 'table')."
    )
    level: Optional[int] = Field(
        None,
        description=
        "Semantic level if applicable (e.g., 0 for PART, 1 for ITEM, heading levels)."
    )
    section_identifier: Optional[str] = Field(
        None,
        description=
        "Standardized identifier (e.g., 'part1', 'part1item1a') if applicable."
    )
    synthetic: bool = Field(
        False,
        description=
        "True if this node was synthesized (e.g., a missing PART title).")
    inferred_from_item_hash: Optional[int] = Field(
        None,
        description=
        "If synthetic, the hash of the item element that triggered the synthesis."
    )
    processing_log: Optional[
        List[Any]] = None  # Ensure this matches how log_data is passed
    text_md5: Optional[str] = None
    model_config = ConfigDict(extra='ignore')
    # Add other potentially useful fields from sec-parser elements as needed
    # e.g., visual_style_summary: Optional[Dict[str, Any]] = None

    # Avoid storing the raw sec-parser element object directly here
    # as it might not be easily serializable or could be very large.
    # source_element_object: Optional[Any] = Field(None, exclude=True) # Example if you needed it temporarily


# Your existing FinLensNode definition should look something like this:
class FinLensNode(BaseModel):
    """Represents a node in the FinLens hierarchical structure."""
    node_id: str = Field(..., description="Unique identifier for the node.")
    parent_node_id: Optional[str] = Field(
        None, description="ID of the parent node, None for root.")
    doc_source_id: str = Field(
        ...,
        description=
        "Identifier for the source document (e.g., accession number).")
    node_type: str = Field(
        ...,
        description=
        "Type of the node (e.g., DOCUMENT_ROOT, PART_TITLE, ITEM_TITLE, TEXT, TABLE)."
    )
    level: int = Field(
        ...,
        description=
        "Hierarchical level of the node (0 for root, 1 for PART, etc.).")
    title: str = Field(...,
                       description="A concise title or heading for the node.")
    section_id: Optional[str] = Field(
        None,
        description="Standardized section ID (e.g., 'part1item5', 'root').")

    # Document metadata duplicated for easier access during retrieval/display
    cik: Optional[str] = Field(None)
    form_type: Optional[str] = Field(None)
    filing_date: Optional[str] = Field(
        None)  # Store as string ISO format YYYY-MM-DD
    fiscal_year_end_date: Optional[str] = Field(
        None)  # Store as string ISO format YYYY-MM-DD

    # Content fields (mutually exclusive depending on node_type)
    text_content: Optional[str] = Field(
        None, description="Text content for TEXT nodes.")
    table_data: Optional[Union[str, List[Dict[str, Any]]]] = Field(
        None,
        description="Table content (e.g., markdown string or JSON structure).")
    # Add fields for images, etc. if needed

    # Metadata linking back to sec-parser processing
    sec_metadata: Optional[SecParserMetadata] = Field(
        None, description="Metadata from the sec-parser process."
    )  # Use the new class here

    # Child relationships are typically handled by querying based on parent_node_id,
    # but can be included for tree structures if preferred.
    children: List['FinLensNode'] = Field(
        default_factory=list, exclude=True
    )  # Exclude from direct serialization if managed separately

    model_config = ConfigDict(
        extra='allow')  # Allow extra fields if needed during processing


# Update forward refs if using Python < 3.10 or complex nesting
# FinLensNode.model_rebuild()
