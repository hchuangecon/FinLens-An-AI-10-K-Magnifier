# src/phase2_parsing/node_builder.py
import logging
import uuid
import re
from typing import List, Optional, Dict, Any, Tuple, TYPE_CHECKING, Type
from pydantic import BaseModel, Field, ConfigDict, ValidationError

# --- Define Availability Flag ---
DOCLING_AVAILABLE = True

# --- Conditional Import for Type Hinting ---
if TYPE_CHECKING:
    from docling.datamodel.document import DoclingDocument
    HintType = DoclingDocument
    from docling_core.types.doc import DocItemLabel, TableItem, TableData, TableCell, DocItem, ProvenanceItem
else:
    HintType = Any
# --- End Conditional Import ---

# --- Runtime Import Attempt ---
try:
    from docling_core.types.doc import DocItemLabel as _RuntimeDocItemLabel
    from docling_core.types.doc import TableItem as _RuntimeTableItem
    from docling_core.types.doc import TableData as _RuntimeTableData
    from docling_core.types.doc import TableCell as _RuntimeTableCell
    from docling_core.types.doc import DocItem as _RuntimeDocItem
    from docling_core.types.doc import ProvenanceItem as _RuntimeProvenanceItem
    logging.info("Successfully imported Docling core types for NodeBuilder.")
except ImportError as e:
    logging.error(f"Docling core types not found: {e}.")
    DOCLING_AVAILABLE = False
    _RuntimeDocItemLabel = None
    _RuntimeTableItem = None
    _RuntimeTableData = None
    _RuntimeTableCell = None
    _RuntimeDocItem = None
    _RuntimeProvenanceItem = None
# --- End Runtime Import Attempt ---

logger = logging.getLogger(__name__)


# --- FinLensNode Definition ---
class FinLensNode(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    node_id: str = Field(...)
    parent_node_id: Optional[str] = Field(None)
    doc_source_id: str = Field(...)
    node_type: str = Field(...)
    level: int = Field(
        ..., description="Semantic hierarchy level (e.g., Part=1, Item=2)")
    title: Optional[str] = Field(None)
    section_id: Optional[str] = Field(None)
    text_content: Optional[str] = Field(None)
    table_data: Optional[str] = Field(None)
    docling_element_ids: List[str] = Field(default_factory=list)
    cik: Optional[str] = None
    form_type: Optional[str] = None
    filing_date: Optional[Any] = None
    fiscal_year_end_date: Optional[Any] = None


# --- Helper Functions ---
def sanitize_for_section_id(text: str, prefix: str = "sec") -> str:
    if not text: return f"{prefix}_unknown"
    s = text.lower()
    s = re.sub(r'^\s*part\s+([ivx]+)\s*[:.]?',
               r'part_\1',
               s,
               flags=re.IGNORECASE)
    s = re.sub(r'^\s*item\s+([\d]+[a-z]?)\.?\s*',
               r'item_\1',
               s,
               flags=re.IGNORECASE)
    s = re.sub(r'\s+', '_', s)
    s = re.sub(r'[()]', '', s)
    s = re.sub(r'[^a-z0-9_]', '', s)
    s = s.strip('_')
    return f"{prefix}_{s[:50]}" if s else f"{prefix}_unknown"


def format_table_data_to_markdown(
        table_data_obj: Optional[Any]) -> Optional[str]:
    RuntimeTableData = globals().get('_RuntimeTableData')
    RuntimeTableCell = globals().get('_RuntimeTableCell')
    if not DOCLING_AVAILABLE or not RuntimeTableData or not RuntimeTableCell:
        return None
    if not isinstance(table_data_obj, RuntimeTableData): return None
    num_cols = getattr(table_data_obj, 'num_cols', 0)
    grid = getattr(table_data_obj, 'grid', [])
    if num_cols == 0 or not grid: return None
    markdown_rows = []
    header_sep = "|".join([" --- "] * num_cols)
    markdown_rows.append(
        f"| {' | '.join(['Col '+str(i+1) for i in range(num_cols)])} |")
    markdown_rows.append(f"|{header_sep}|")
    for row_cells in grid:
        row_texts = []
        for cell in row_cells:
            if isinstance(cell, RuntimeTableCell):
                cell_text = getattr(cell, 'text', '')
                cleaned_text = cell_text.replace('|', '\\|').replace(
                    '\n', ' ').replace('\r', '').strip()
                row_texts.append(f" {cleaned_text if cleaned_text else ' '} ")
            else:
                row_texts.append(" ? ")
        while len(row_texts) < num_cols:
            row_texts.append(' ')
        markdown_rows.append(f"|{'|'.join(row_texts[:num_cols])}|")
    return "\n".join(markdown_rows)


class HierarchicalNodeBuilder:
    """ Builds the FinLensNode tree from a DoclingDocument. """

    def __init__(self):
        logger.info("HierarchicalNodeBuilder initialized.")
        self.node_counter = 0

    def _generate_node_id(self) -> str:
        self.node_counter += 1
        return f"node_{self.node_counter}"

    def _get_heading_level_heuristic(self, item: Any) -> int:
        """
        Determine semantic heading level:
        - PART I, II, III => Level 1
        - ITEM N (no letter) => Level 2
        - ITEM N with letter suffix (e.g., 1A) => Level 3
        """
        text = getattr(item, 'text', '').strip()
        # Part headings
        if re.match(r'^\s*PART\s+[IVX]+', text, re.IGNORECASE):
            return 1
        # Item with letter suffix
        m = re.match(r'^\s*ITEM\s+(\d+)([A-Z])', text, re.IGNORECASE)
        if m:
            return 3
        # Item without suffix
        if re.match(r'^\s*ITEM\s+\d+\b', text, re.IGNORECASE):
            return 2
        # Fallback for other headings
        return 4

    def _get_current_section_id(
            self, node_stack: List[Tuple[FinLensNode, int]]) -> Optional[str]:
        for node, _level in reversed(node_stack):
            if node.node_type == "HEADING" and node.section_id:
                return node.section_id
        return "root"

    def build_tree(
        self, doc: HintType, doc_metadata: Dict[str, Any]
    ) -> Tuple[List[FinLensNode], Optional[FinLensNode]]:
        self.node_counter = 0
        nodes_list = []
        root_node = None
        node_stack: List[Tuple[FinLensNode, int]] = []

        if not DOCLING_AVAILABLE or not doc or not hasattr(
                doc, 'iterate_items'):
            logger.error("Docling unavailable or invalid doc.")
            return [], None
        RuntimeTableItem = globals().get('_RuntimeTableItem')
        RuntimeDocItemLabel = globals().get('_RuntimeDocItemLabel')
        if not RuntimeTableItem or not RuntimeDocItemLabel:
            logger.error("Runtime types missing.")
            return [], None

        doc_source_id = doc_metadata.get('accession_number', 'unknown_doc')
        # Root node
        root_data = {
            "node_id": self._generate_node_id(),
            "parent_node_id": None,
            "doc_source_id": doc_source_id,
            "node_type": "DOCUMENT",
            "level": 0,
            "title": doc_metadata.get('name', doc_source_id),
            "section_id": "root",
            **doc_metadata
        }
        root_node = FinLensNode(**root_data)
        nodes_list.append(root_node)
        node_stack.append((root_node, 0))

        for item, _ in doc.iterate_items():
            label = getattr(item, 'label', None)
            node_type = "UNKNOWN"
            title = None
            text_content = None
            table_data = None
            semantic_level = None
            section_id = None

            # Determine type
            if isinstance(item, RuntimeTableItem):
                node_type = "TABLE"
                table_data = format_table_data_to_markdown(
                    getattr(item, 'data', None))
            elif label == _RuntimeDocItemLabel.SECTION_HEADER:
                node_type = "HEADING"
                title = getattr(item, 'text', '').strip()
                semantic_level = self._get_heading_level_heuristic(item)
                section_id = sanitize_for_section_id(
                    title, prefix=f"h{semantic_level}")
            else:
                continue

            # Determine semantic_level for non-heading
            if semantic_level is None:
                semantic_level = node_stack[-1][1] + 1
            # Manage stack for headings
            if node_type == "HEADING":
                while node_stack and semantic_level <= node_stack[-1][1]:
                    node_stack.pop()
            parent_node = node_stack[-1][0]
            parent_level = node_stack[-1][1]
            parent_id = parent_node.node_id

            # Inherit section_id if not heading
            if node_type != "HEADING":
                section_id = self._get_current_section_id(node_stack)

            node_data = {
                "node_id":
                self._generate_node_id(),
                "parent_node_id":
                parent_id,
                "doc_source_id":
                doc_source_id,
                "node_type":
                node_type,
                "level":
                semantic_level,
                "title":
                title,
                "section_id":
                section_id,
                "text_content":
                text_content,
                "table_data":
                table_data,
                "docling_element_ids":
                [str(getattr(item, 'provenance_id', getattr(item, 'id', '')))],
                **doc_metadata
            }
            try:
                node = FinLensNode(**node_data)
                nodes_list.append(node)
                if node_type == "HEADING":
                    node_stack.append((node, semantic_level))
            except ValidationError as e:
                logger.error(f"Node validation error: {e}")

        logger.info(f"Built {len(nodes_list)-1} nodes for {doc_source_id}")
        return nodes_list, root_node
