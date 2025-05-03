# src/phase2_parsing/node_builders/sec_parser_node_builder.py
# (Imports and other methods remain the same as the previous correct version)
# ... imports ...
import logging
import re
import sys
import bs4  # Keep this if needed, though HtmlTag encapsulates it
import uuid
import pandas as pd
from io import StringIO
from typing import Dict, List, Optional, Tuple, Any, Union, cast

logger = logging.getLogger(__name__)

from pydantic import ValidationError

# --- sec-parser Imports (Corrected in previous step) ---
SEC_PARSER_AVAILABLE = False
try:
    from sec_parser import SemanticTree, TreeBuilder
    from sec_parser.semantic_elements.abstract_semantic_element import AbstractSemanticElement
    from sec_parser.semantic_elements.composite_semantic_element import CompositeSemanticElement
    from sec_parser.semantic_elements import TitleElement  # From package level
    from sec_parser.semantic_elements import (
        TextElement,
        TableElement,
        ImageElement,  # TableElement needed here
        IrrelevantElement,
        NotYetClassifiedElement,
        EmptyElement,
        SupplementaryText)
    from sec_parser.semantic_elements import TopSectionTitle  # From package level
    # Import TableOfContentsElement if needed for specific checks
    from sec_parser.semantic_elements.table_element.table_of_contents_element import TableOfContentsElement
    from sec_parser.semantic_tree.tree_node import TreeNode

    SEC_PARSER_AVAILABLE = True
    logger.debug(
        "Local sec-parser types imported successfully for SecParserNodeBuilder."
    )
except ImportError as e:
    logger.critical(f"Failed to import from local sec-parser library: {e}")
    SEC_PARSER_AVAILABLE = False
except Exception as e:
    logger.critical(f"An unexpected error during local sec-parser import: {e}",
                    exc_info=True)
    SEC_PARSER_AVAILABLE = False

# --- FinLens Imports (Remain the same) ---
try:
    from ..types.interfaces import AbstractNodeBuilder
    from ..types.models import FinLensNode, SecParserMetadata
except ImportError as e:
    logger.critical(
        f"Failed to import FinLens types for SecParserNodeBuilder: {e}")
    AbstractNodeBuilder = object

# --- Helper Function & Class Definition (Remain the same) ---
_ROMAN_MAP = {"1": "I", "2": "II", "3": "III", "4": "IV", "5": "V"}


def sanitize_for_section_id(text: str, prefix: str = "sec") -> str:
    # ... (function body) ...
    if not text:
        return f"{prefix}_unknown"
    s = text.strip().lower()
    part_match = re.match(r'^\s*part\s+([ivxlc]+)\b', s)
    item_match = re.match(r'^\s*item\s+(\d+[a-z]?)\b', s)
    if part_match:
        num = {
            "i": "1",
            "ii": "2",
            "iii": "3",
            "iv": "4"
        }.get(part_match.group(1), part_match.group(1))
        return f"part{num}"
    elif item_match:
        item_num_letter = item_match.group(1)
        return f"item{item_num_letter}"
    else:
        sanitized = re.sub(r'[^a-z0-9_]+', '', re.sub(r'\s+', '_',
                                                      s)).strip('_')
        identifier = sanitized[:50]
    return f"{prefix}_{identifier}" if identifier else f"{prefix}_unknown"


class SecParserNodeBuilder(AbstractNodeBuilder):

    def __init__(self) -> None:  # (Remains the same)
        self.node_counter: int = 0
        self.current_doc_id: str = "unknown_doc"
        self.finlens_nodes: List[FinLensNode] = []
        self.current_part_finlens_node: Optional[FinLensNode] = None
        self.doc_meta: Dict[str, Any] = {}
        if not SEC_PARSER_AVAILABLE or not FinLensNode or not SecParserMetadata:
            logger.error(
                "SecParserNodeBuilder initialized, but components unavailable."
            )
        logger.info("SecParserNodeBuilder initialized.")

    def _generate_node_id(self) -> str:
        return str(uuid.uuid4())  # (Remains the same)

    def _get_section_identifier(
        self, element: Optional[Union[AbstractSemanticElement, FinLensNode]]
    ) -> Optional[str]:  # (Remains the same)
        if not element: return None
        if isinstance(element, AbstractSemanticElement):
            section_type = getattr(element, 'section_type', None)
            return getattr(section_type, 'identifier',
                           None) if section_type else None
        elif isinstance(element, FinLensNode):
            return element.sec_metadata.section_identifier if element.sec_metadata else None
        return None

    def build_tree(
        self, sec_parser_tree: SemanticTree, doc_meta: Dict[str, Any]
    ) -> Tuple[List[FinLensNode], Optional[FinLensNode]]:
        """
        Builds the FinLensNode tree from a sec-parser SemanticTree.

        Args:
            sec_parser_tree: The input tree from sec-parser.
            doc_meta: Dictionary containing document metadata.

        Returns:
            A tuple containing the flat list of all created FinLensNode objects
            and the root FinLensNode object.
        """
        # Initialize state for a new build
        self.node_counter = 0
        self.finlens_nodes = []
        self.current_part_finlens_node = None  # Reset part context
        self.doc_meta = doc_meta
        root_node: Optional[FinLensNode] = None

        # Check dependencies
        if not SEC_PARSER_AVAILABLE or not FinLensNode or not SecParserMetadata:
            logger.error(
                "Cannot build tree: Essential components (sec-parser, FinLens models) unavailable."
            )
            return [], None
        if not isinstance(sec_parser_tree, SemanticTree):
            logger.error(
                f"Invalid input: Expected SemanticTree, got {type(sec_parser_tree)}"
            )
            return [], None

        # Set document identifier
        self.current_doc_id = self.doc_meta.get(
            "accession_number",  # Prefer accession number
            self.doc_meta.get("filename_base",
                              "unknown_doc")  # Fallback to filename
        )
        logger.info(
            f"Starting SecParserNodeBuilder for doc: {self.current_doc_id}")

        # --- Create Root FinLens Node ---
        try:
            root_title = self.doc_meta.get(
                "document_type",
                self.current_doc_id  # Use form type (e.g., 10-K)
            ) or self.current_doc_id  # Fallback to doc_id if type unknown
            root_node_id = self._generate_node_id()
            root_node = FinLensNode(
                node_id=root_node_id,
                parent_node_id=None,  # Root has no parent
                doc_source_id=self.current_doc_id,
                node_type="DOCUMENT_ROOT",
                level=0,  # Root is level 0
                order_in_parent=0,  # Root is the first/only at its level
                title=root_title,
                section_id="root",  # Fixed section ID for root
                cik=self.doc_meta.get("cik"),
                form_type=self.doc_meta.get("form_type"),
                filing_date=self.doc_meta.get("filing_date"),
                fiscal_year_end_date=self.doc_meta.get("fiscal_year_end_date"),
                sec_metadata=
                None  # Root doesn't directly map to sec-parser element
            )
            self.finlens_nodes.append(root_node)
            self.doc_meta[
                "root_node_id"] = root_node_id  # Store for potential use
            logger.debug(f"Created DOCUMENT_ROOT node ID: {root_node_id}")
        except Exception as root_e:
            logger.error(
                f"Error creating root FinLensNode for {self.current_doc_id}: {root_e}",
                exc_info=True)
            return [], None  # Cannot proceed without a root node

        # --- Traverse sec-parser Tree ---
        # Iterate through the top-level nodes provided by sec-parser's SemanticTree
        logger.info(
            f"Starting traversal of {sec_parser_tree.nodes} top-level sec-parser nodes..."
        )
        for sec_root_node in sec_parser_tree.nodes:
            root_element = sec_root_node.semantic_element
            logger.debug(
                f"BUILD_TREE LOOP: Considering sec_root_node for element type {type(root_element).__name__}"
            )

            # --- ADDED CHECK: Skip unexpected top-level Items/Titles ---
            # We only expect Parts (level 0) or perhaps introductory/unclassified elements
            # at the top level of the sec_parser tree structure after TreeBuilder.
            # If we find an Item (level 1+) here, it's likely a duplicate/structural issue
            # in the input tree, and we should skip processing it directly from here,
            # assuming it was already processed correctly via recursion under its Part node.
            if isinstance(root_element,
                          TopSectionTitle) and root_element.level >= 1:
                #  logger.warning(
                #      f"BUILD_TREE LOOP: Skipping unexpected top-level Item/Title element (Level {root_element.level}): {root_element.text[:60]}..."
                #  )
                continue  # Skip calling _traverse for this potentially duplicate node
            # --- END CHECK ---

            # Process the node if it wasn't skipped (e.g., it's a Part title or other root element)
            logger.debug(
                f"BUILD_TREE LOOP: Calling _traverse for {type(root_element).__name__} at initial level 1"
            )
            try:
                # Start traversal for this top-level section, parent is the document root
                # Children of the root are level 1
                self._traverse(sec_root_node,
                               parent_finlens_node_id=root_node.node_id,
                               level=1)
            except Exception as traverse_e:
                # Catch potential errors during traversal of a specific branch
                logger.error(
                    f"Error during _traverse call for root element {type(root_element).__name__}: {traverse_e}",
                    exc_info=True)
                # Decide whether to continue with the next root node or stop
                # continue # Example: Continue processing other top-level nodes

        # --- Final Log and Return ---
        logger.info(
            f"Finished building tree for {self.current_doc_id}, created {len(self.finlens_nodes)} FinLensNodes (including root)."
        )
        # Return the flat list and the root node object
        return self.finlens_nodes, root_node

    def _traverse(self, sec_node: TreeNode, parent_finlens_node_id: str,
                  level: int):
        """
        Recursively traverses the sec-parser semantic tree and builds the FinLensNode list.
        (Cleaned logging + Revised Parent ID Override Fix)
        """
        element = sec_node.semantic_element
        logger.debug(
            f"TRAVERSE_ENTER: Processing element type {type(element).__name__} at level {level}. Expecting parent {parent_finlens_node_id}."
        )

        actual_parent_id = parent_finlens_node_id  # Parent ID from the input tree structure
        current_element_is_part = False

        if isinstance(element, TopSectionTitle):
            element_sec_id = self._get_section_identifier(element)

            # --- Handle Part Titles (Level 0) ---
            if element.level == 0:
                current_element_is_part = True
                logger.debug(
                    f"  Part Title Detected: '{element.text.strip()}', SectionId='{element_sec_id}'"
                )
                # The current_part_finlens_node will be updated AFTER this node is created

            # --- Handle Item Titles (Level >= 1) ---
            elif element.level >= 1:
                item_sec_id = element_sec_id
                match = re.match(r"(part\d+)(item.+)", item_sec_id or "")
                if match:
                    expected_part_identifier = match.group(1)
                    current_part_context_id = self._get_section_identifier(
                        self.current_part_finlens_node)
                    current_part_context_node_id = self.current_part_finlens_node.node_id if self.current_part_finlens_node else None

                    logger.debug(
                        f"  Item Check: ItemSecID='{item_sec_id}', ExpectedPart='{expected_part_identifier}', CurrentPartContext='{current_part_context_id}' (NodeID: {current_part_context_node_id}), Input Parent='{parent_finlens_node_id}'"
                    )

                    # --- Case 1: Context Mismatch (Item belongs to a DIFFERENT Part than current context) ---
                    if expected_part_identifier != current_part_context_id:
                        logger.info(
                            f"  Context Mismatch for {item_sec_id}. Current: {current_part_context_id}, Expected: {expected_part_identifier}."
                        )
                        # Find or Synthesize the CORRECT Part node
                        target_part_node = self._find_or_synthesize_part(
                            expected_part_identifier, element)
                        if target_part_node:
                            self.current_part_finlens_node = target_part_node  # Update context
                            actual_parent_id = target_part_node.node_id  # Set parent to CORRECT part
                            logger.info(
                                f"  Context updated to '{expected_part_identifier}' (Node {target_part_node.node_id}). Set parent for {item_sec_id} to {actual_parent_id}."
                            )
                        else:
                            logger.error(
                                f"  Failed to find or synthesize target part '{expected_part_identifier}' for {item_sec_id}. Parent remains {actual_parent_id}."
                            )

                    # --- Case 2: Context Matches (Item belongs to the CURRENT Part) ---
                    elif current_part_context_node_id is not None:  # Ensure context isn't None
                        # Check if the parent from recursion matches the current context node ID
                        if parent_finlens_node_id != current_part_context_node_id:
                            # logger.warning(
                            #     f"  Parent INCONSISTENCY for {item_sec_id}: Expected parent based on context = {current_part_context_node_id}, but input parent from tree = {parent_finlens_node_id}. Overriding parent."
                            # )
                            actual_parent_id = current_part_context_node_id  # Override with the correct context parent ID
                        # else: Parent matches context, proceed as normal

        # --- Mapping the current element ---
        current_order = self.node_counter
        logger.debug(
            f"  Calling map function for element {type(element).__name__} with PARENT={actual_parent_id}, level={level}, order={current_order}"
        )
        finlens_node = self._map_element_to_finlens_node(
            element,
            actual_parent_id,
            level,
            current_order  # Use the potentially corrected actual_parent_id
        )

        # --- Append and Update Context ---
        if finlens_node:
            self.finlens_nodes.append(finlens_node)
            self.node_counter += 1
            logger.debug(
                f"  MAP_SUCCESS: Node ID={finlens_node.node_id}, Type={finlens_node.node_type}, Title='{finlens_node.title}', SectionId={finlens_node.section_id}, Level={finlens_node.level}, Parent={finlens_node.parent_node_id}"
            )

            # Update context AFTER creating the node if it was a Part title
            if current_element_is_part:
                self.current_part_finlens_node = finlens_node
                logger.info(
                    f"  PART CONTEXT UPDATED: Set to Node ID={finlens_node.node_id}, SectionId={finlens_node.section_id}, Title='{finlens_node.title}'"
                )

            # --- Recursive Call ---
            logger.debug(
                f"  RECURSE_PARENT_INFO: Node ID={finlens_node.node_id}")
            if hasattr(sec_node, 'children') and sec_node.children:
                for child_sec_node in sec_node.children:
                    logger.debug(
                        f"    -> RECURSE_CHILD: Child Type='{type(child_sec_node.semantic_element).__name__}', Passing Parent ID='{finlens_node.node_id}', Passing Level={level + 1}"
                    )
                    self._traverse(
                        child_sec_node,
                        parent_finlens_node_id=finlens_node.
                        node_id,  # Parent for children is the current node
                        level=level + 1)
            else:
                logger.debug(
                    f"  Node ID={finlens_node.node_id} has no children.")

        else:  # if finlens_node is None
            # logger.warning(
            #     f"  MAP_FAILED: Element type {type(element).__name__} at level {level}. Assigned Parent was {actual_parent_id}."
            # )
            logger.debug(
                f"  Attempting recursion for children of failed mapping node, using original parent {parent_finlens_node_id}"
            )
            if hasattr(sec_node, 'children') and sec_node.children:
                for child_sec_node in sec_node.children:
                    logger.debug(
                        f"    -> RECURSE_CHILD (Parent Skipped): Child Type='{type(child_sec_node.semantic_element).__name__}', Passing Parent ID='{parent_finlens_node_id}', Passing Level={level + 1}"
                    )
                    self._traverse(
                        child_sec_node,
                        parent_finlens_node_id=parent_finlens_node_id,
                        level=level + 1)

    # --- Helper function for finding/synthesizing Part ---
    def _find_or_synthesize_part(
            self, expected_part_identifier: str,
            triggering_element: AbstractSemanticElement
    ) -> Optional[FinLensNode]:
        """Finds an existing node for the part, or synthesizes it if not found."""
        # Determine parent ID for the Part node (should be root)
        part_parent_id = self.doc_meta.get("root_node_id", "root")

        # Look for existing node first
        existing_node = next((n for n in self.finlens_nodes
                              if n.section_id == expected_part_identifier
                              and n.parent_node_id == part_parent_id), None)
        if existing_node:
            logger.info(
                f"  Found existing node {existing_node.node_id} for target part {expected_part_identifier}."
            )
            return existing_node
        else:
            # Synthesize if not found
            logger.info(
                f"  Existing node for {expected_part_identifier} not found, synthesizing."
            )
            synthetic_part_node = self._synthesize_part_node(
                expected_part_identifier, triggering_element, part_parent_id)
            if synthetic_part_node:
                # Check again VERY thoroughly if an equivalent was added JUST now by another process/recursion branch?
                # This check might be overly cautious or complex. For now, let's append if synthesized.
                exists_check_after_synth = any(
                    n.node_id == synthetic_part_node.node_id
                    for n in self.finlens_nodes)
                if not exists_check_after_synth:
                    self.finlens_nodes.append(synthetic_part_node)
                    logger.info(
                        f"  SYNTHESIZED Part Node Added: ID={synthetic_part_node.node_id}, SectionId={synthetic_part_node.section_id}"
                    )
                    return synthetic_part_node
                else:
                    logger.warning(
                        f" Synthesized node {synthetic_part_node.node_id} for {expected_part_identifier} already in list immediately after creation? Using it anyway."
                    )
                    return synthetic_part_node  # Return the synthesized one even if maybe added twice? Risky. Let's return None if confused.
                    # return None # Safer to return None if potentially duplicated

            else:
                logger.error(
                    f"  Synthesis function failed for {expected_part_identifier}."
                )
                return None
        return None  # Should have returned earlier

    def _synthesize_part_node(
            self, expected_part_identifier: str,
            triggering_element: AbstractSemanticElement,
            parent_finlens_node_id: str) -> Optional[FinLensNode]:
        """
        Creates a synthetic FinLensNode for a Part section when an Item
        is found outside its expected Part context.
        (Logging levels reviewed)
        """
        if not expected_part_identifier:
            logger.warning(
                "Cannot synthesize part node without an expected identifier.")
            return None

        logger.info(  # Keep synthesis log as INFO
            f"Synthesizing missing Part node for identifier: {expected_part_identifier}"
        )

        part_num_match = re.match(r'part(\d+)', expected_part_identifier)
        part_title = f"Part {expected_part_identifier.capitalize()}"
        if part_num_match:
            part_num_str = part_num_match.group(1)
            roman_numeral = _ROMAN_MAP.get(part_num_str, part_num_str)
            part_title = f"PART {roman_numeral}"

        html_tag_source = getattr(triggering_element, 'html_tag', None)
        bs4_tag_for_meta = None
        if html_tag_source and hasattr(html_tag_source, '_bs4'):
            bs4_tag_for_meta = getattr(html_tag_source, '_bs4', None)

        sec_metadata = None
        try:
            triggering_text = triggering_element.text if hasattr(
                triggering_element, 'text') else None
            computed_source_hash = hash(
                triggering_text) if triggering_text else None

            # Ensure field names match your Pydantic model definition
            sec_metadata = SecParserMetadata(
                source_element_type="SynthesizedPartTitle",
                source_element_hash=computed_source_hash,
                tag_name=bs4_tag_for_meta.name if bs4_tag_for_meta else None,
                level=0,
                section_identifier=expected_part_identifier,
                synthetic=True,
                inferred_from_item_hash=hash(triggering_text)
                if triggering_text else None,
                processing_log=None,
                text_md5=None)
        except ValidationError as e:
            logger.error(
                f"Validation Error creating SecParserMetadata for synthesized part '{expected_part_identifier}': {e}"
            )
            sec_metadata = None
        except Exception as e:
            logger.error(
                f"Unexpected Error creating SecParserMetadata for synthesized part '{expected_part_identifier}': {e}",
                exc_info=True)
            sec_metadata = None

        try:
            node_id = self._generate_node_id()
            synthetic_level = 1

            synthetic_node = FinLensNode(
                node_id=node_id,
                parent_node_id=parent_finlens_node_id,
                doc_source_id=self.current_doc_id,
                node_type="SECTION_TITLE",
                level=synthetic_level,
                order_in_parent=self.node_counter,
                title=part_title,
                section_id=expected_part_identifier,
                text_content=None,
                cik=self.doc_meta.get("cik"),
                form_type=self.doc_meta.get("form_type"),
                filing_date=self.doc_meta.get("filing_date"),
                fiscal_year_end_date=self.doc_meta.get("fiscal_year_end_date"),
                sec_metadata=sec_metadata,
                extra_attributes={"synthesized": True})
            logger.debug(  # Keep node creation log as DEBUG
                f"Successfully created synthesized Part node: ID={node_id}, Title='{part_title}', Parent={parent_finlens_node_id}, Level={synthetic_level}"
            )
            return synthetic_node
        except ValidationError as e:
            logger.error(
                f"Validation Error creating FinLensNode for synthesized part '{expected_part_identifier}': {e}"
            )
            return None
        except Exception as e:
            logger.error(
                f"Unexpected error creating FinLensNode for synthesized part '{expected_part_identifier}': {e}",
                exc_info=True)
            return None

    def _map_element_to_finlens_node(
            self, element: AbstractSemanticElement,
            parent_finlens_node_id: str, level: int,
            order_in_parent: int) -> Optional[FinLensNode]:
        """
        Maps a sec-parser semantic element to a FinLensNode.

        Args:
            element: The sec-parser semantic element to map.
            parent_finlens_node_id: The ID of the parent FinLensNode.
            level: The hierarchical level for the new FinLensNode.
            order_in_parent: The sequential order of this node among siblings.

        Returns:
            A FinLensNode instance or None if the element should be skipped or mapping fails.
        """
        # Skip irrelevant, empty, unclassified, or ToC elements if desired
        if not element or isinstance(
                element, (IrrelevantElement, EmptyElement,
                          NotYetClassifiedElement, TableOfContentsElement)):
            logger.debug(f"Skipping element type {type(element).__name__}")
            return None

        node_id = self._generate_node_id()
        node_type = "UNKNOWN"
        title: Optional[str] = None
        text_content: Optional[str] = None
        section_id: Optional[str] = None
        sec_metadata: Optional[SecParserMetadata] = None
        extra_attrs: Dict[str, Any] = {}

        # --- Determine Node Type and Extract Content ---
        try:
            # --- Type specific extraction ---
            if isinstance(element, (TopSectionTitle, TitleElement)):
                node_type = "SECTION_TITLE"
                title = element.text.strip()
                section_id = self._get_section_identifier(element)
                if not section_id and title:
                    section_id = sanitize_for_section_id(title)

            elif isinstance(element, TextElement):
                node_type = "TEXT"
                text_content = element.text.strip()
                if not text_content:
                    logger.debug("Skipping TextElement with empty content.")
                    return None

            elif isinstance(element, TableElement):
                node_type = "TABLE"
                text_content = None  # Default to None
                extra_attrs = {}  # Reset extra_attrs for table
                try:
                    html_tag_wrapper = getattr(element, 'html_tag', None)
                    if html_tag_wrapper:
                        # Ensure '_bs4' is the correct attribute for the bs4.Tag object
                        actual_bs4_tag_attribute = '_bs4'
                        if hasattr(html_tag_wrapper, actual_bs4_tag_attribute):
                            bs4_tag = getattr(html_tag_wrapper,
                                              actual_bs4_tag_attribute)
                            html_string = str(bs4_tag)  # Get the HTML string

                            # Wrap HTML string in StringIO for pandas < 2.1 compatibility if needed
                            # For pandas >= 2.1, you can often pass the string directly
                            html_io = StringIO(html_string)

                            # Use pandas to read tables from the HTML string
                            # read_html returns a LIST of DataFrames found
                            dfs = pd.read_html(
                                html_io, flavor='lxml',
                                header=0)  # Use lxml, assume header=0

                            if dfs:
                                # Assume the first table found is the one we want
                                df = dfs[0]
                                # Clean up DataFrame (optional: fill NaN, drop empty rows/cols)
                                # df = df.dropna(how='all').dropna(axis=1, how='all') # Example cleanup

                                # Convert DataFrame to Markdown for text_content
                                text_content = df.to_markdown(index=False)

                                # Optional: Store structured data in extra_attrs (or table_data if model has it)
                                # extra_attrs['table_data_list'] = df.values.tolist()

                                logger.debug(
                                    f"Successfully parsed table and converted to Markdown."
                                )
                            else:
                                logger.warning(
                                    "Pandas read_html did not find any tables in the HTML string."
                                )
                                text_content = "[Table Found - No Data Parsed by Pandas]"
                        else:
                            logger.warning(
                                f"Attribute '{actual_bs4_tag_attribute}' not found on HtmlTag wrapper for TableElement."
                            )
                            text_content = "[Table HTML structure not accessible]"
                    else:
                        logger.warning(
                            "Could not find html_tag wrapper for TableElement."
                        )
                        text_content = "[Table HTML wrapper not found]"

                except ImportError:
                    logger.error(
                        "Pandas or lxml not installed. Cannot parse table. Install with 'pip install pandas lxml'. Storing raw HTML."
                    )
                    # Fallback to raw HTML if pandas/lxml fails or isn't installed
                    if 'bs4_tag' in locals(): text_content = str(bs4_tag)
                    else:
                        text_content = "[Table HTML processing error - libraries missing?]"

                except Exception as e:
                    logger.error(
                        f"Error processing TableElement with Pandas: {e}",
                        exc_info=True)
                    # Fallback to raw HTML on other errors
                    if 'bs4_tag' in locals(): text_content = str(bs4_tag)
                    else: text_content = "[Table Processing Error]"
                    extra_attrs["error"] = str(e)  # Log error in attributes

            elif isinstance(element, ImageElement):
                node_type = "IMAGE"
                # title = "Image" # Optional placeholder title
            elif isinstance(element, SupplementaryText):
                node_type = "SUPPLEMENTARY"  # Or "SUPPLEMENTARY_TEXT"
                text_content = element.text.strip()
                if not text_content:
                    logger.debug(
                        "Skipping SupplementaryText with empty content.")
                    return None
                # Supplementary text usually doesn't have a title or section_id
                title = None
                section_id = None
            else:
                # Fallback for other types
                logger.warning(
                    f"Unhandled element type: {type(element).__name__}. Mapping as UNKNOWN."
                )
                node_type = "OTHER"
                text_content = element.text.strip() if hasattr(
                    element, 'text') else None

            # --- Create SecParserMetadata ---
            processing_log = getattr(element, 'processing_log', None)

            log_data = []
            processing_log = getattr(element, 'processing_log', None)

            log_data = []
            if processing_log:
                # ACTION POINT 2: Implement correct logic using 'get_items'
                try:
                    if hasattr(processing_log, 'get_items'):
                        # Call the get_items() method
                        items = processing_log.get_items()
                        # Convert to list for safety, in case it returns an iterator etc.
                        log_data = list(items)
                    else:
                        # This case should not happen based on dir() output
                        logger.warning(
                            f"'get_items' method not found on ProcessingLog object unexpectedly."
                        )

                except Exception as log_e:
                    logger.warning(
                        f"Error trying to extract ProcessingLog data using get_items(): {log_e}"
                    )

            # Prepare bs4 tag info for metadata =
            html_tag_wrapper_for_meta = getattr(element, 'html_tag', None)
            bs4_tag_for_meta = None
            if html_tag_wrapper_for_meta:
                actual_bs4_tag_attribute_for_meta = '_bs4'
                if hasattr(html_tag_wrapper_for_meta,
                           actual_bs4_tag_attribute_for_meta):
                    bs4_tag_for_meta = getattr(
                        html_tag_wrapper_for_meta,
                        actual_bs4_tag_attribute_for_meta)

            sec_metadata = SecParserMetadata(
                element_type=type(element).__name__,
                section_level=getattr(element, 'level', None),
                section_identifier=self._get_section_identifier(element),
                text_md5=getattr(element, 'md5_hash', None),
                char_count=len(element.text)
                if hasattr(element, 'text') else 0,
                source_html_tag_type=bs4_tag_for_meta.name
                if bs4_tag_for_meta else None,
                source_html_tag_hash=str(hash(str(bs4_tag_for_meta)))
                if bs4_tag_for_meta else None,
                source_html_visible_text_hash=str(hash(element.text))
                if hasattr(element, 'text') and element.text else None,
                processing_log=log_data,  # Use the (hopefully) populated list
            )

        except Exception as e:  # Catch errors during element processing / metadata prep
            logger.error(
                f"Unexpected Error during metadata prep for {type(element).__name__}: {e}",
                exc_info=True)
            sec_metadata = None  # Ensure metadata is None if prep fails

        # --- Create FinLensNode ---
        try:
            # Provide default for title if None (Assumes FinLensNode.title requires str)
            # If FinLensNode.title is Optional[str], you can remove effective_title and just pass title=title
            effective_title = title if title is not None else ""

            finlens_node = FinLensNode(
                node_id=node_id,
                parent_node_id=parent_finlens_node_id,
                doc_source_id=self.current_doc_id,
                node_type=node_type,
                level=level,
                order_in_parent=order_in_parent,
                title=effective_title,  # Use defaulted title
                section_id=section_id,
                text_content=text_content,
                cik=self.doc_meta.get("cik"),
                form_type=self.doc_meta.get("form_type"),
                filing_date=self.doc_meta.get("filing_date"),
                fiscal_year_end_date=self.doc_meta.get("fiscal_year_end_date"),
                sec_metadata=sec_metadata,
                extra_attributes=extra_attrs if extra_attrs else None)
            return finlens_node

        except ValidationError as e:
            logger.error(
                f"Validation Error creating FinLensNode for {type(element).__name__} (ID: {node_id}): {e}"
            )
            return None  # Skip node if validation fails
        except Exception as e:
            logger.error(
                f"Unexpected error creating FinLensNode for {type(element).__name__} (ID: {node_id}): {e}",
                exc_info=True)
            return None
