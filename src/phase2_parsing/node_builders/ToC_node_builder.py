# src/phase2_parsing/ToC_node_builder.py
"""
TOC-first HierarchicalNodeBuilder.
Prioritizes TOC passed via metadata (extracted using raw HTML parser).
Falls back to internal extraction (using DoclingDocument) if no external TOC provided.
Finally falls back to HierarchicalNodeBuilder if all TOC methods fail.

ADDED DEBUG LOGGING to content attachment loop.
CORRECTED ITEM_LINE_RE to use the verbose version.
"""
from __future__ import annotations

import logging  # <<< Added Import
import re
from typing import Dict, List, Optional, Tuple, Any, TYPE_CHECKING

from pydantic import ValidationError

logger = logging.getLogger(__name__)  # <<< Added Logger instance

# --- Docling Imports (remain the same) ---
DOCLING_AVAILABLE = True
try:
    from docling_core.types.doc import DocItemLabel as _RuntimeDocItemLabel
    from docling_core.types.doc import TableItem as _RuntimeTableItem
    from docling_core.types.doc import DoclingDocument as _RuntimeDoclingDocument
    from docling_core.types.doc import TableData as _RuntimeTableData
    from docling_core.types.doc import TableCell as _RuntimeTableCell
except ImportError:
    DOCLING_AVAILABLE = False
    _RuntimeDocItemLabel = None
    _RuntimeTableItem = None
    _RuntimeDoclingDocument = None
    _RuntimeTableData = None
    _RuntimeTableCell = None
    logger.warning(
        "Docling core types not found. TOC Builder functionality may be limited."
    )

if TYPE_CHECKING:
    # Use concrete type hints if docling_core is guaranteed available at runtime
    # and type checkers can resolve it. Otherwise, keep using Any.
    # from docling_core.types.doc import DoclingDocument as HintType
    HintType = Any
else:
    HintType = Any
# --- End Docling Imports ---

from src.phase2_parsing.node_builders.node_builder import (
    FinLensNode,
    sanitize_for_section_id,
    format_table_data_to_markdown,
    HierarchicalNodeBuilder,  # for graceful fallback
)


class TOCHierarchicalNodeBuilder:
    """Build a FinLens node tree by anchoring all content under a TOC outline."""

    MAX_LEVEL = 6
    # <<< CORRECTED REGEX DEFINITION BELOW >>>
    ITEM_LINE_RE = re.compile(
        r'''(?xi)               # ignore case, verbose
        ^\s*(PART\s+[IVXLC]+|ITEM\s+\d+[A-Z]?)   # “PART I”/“PART IV”/“PART X” or “ITEM 1A”/“ITEM 7” (Group 1) - Added L, C
        [\.\-\—:\s]* # separator (dot, hyphen, em dash, colon, space)
        (?P<title>[\w\(\)&/\,\-\s\.'’`]+?)       # section title (named group 'title') - Added . ' ’ `
        # Optional non-capturing group for page number indicators like ... 123 or --- 123
        (?:\s*(?:\.{2,}|\s{2,}|–|—)\s*(?P<page>\d{1,3})?\s*)?
        $''', re.MULTILINE | re.VERBOSE)

    # <<< END CORRECTED REGEX DEFINITION >>>

    def __init__(self) -> None:
        self.node_counter = 0
        logger.info("TOC-based HierarchicalNodeBuilder initialised.")

    def _generate_node_id(self) -> str:
        self.node_counter += 1
        return f"node_{self.node_counter}"

    def _toc_regex(self, txt: str) -> Tuple[bool, int]:
        """Internal helper for fallback TOC check on Docling items. Uses the corrected ITEM_LINE_RE."""
        txt = txt.strip()
        match = self.ITEM_LINE_RE.match(txt)
        if match:
            identifier_group = match.group(1).upper().strip()
            if identifier_group.startswith("PART"):
                return True, 1
            elif identifier_group.startswith("ITEM"):
                # Check if the last character of the ITEM part is a letter
                item_id_part = identifier_group.split()[-1]  # Get '1A' or '7'
                if item_id_part[-1].isalpha():
                    return True, 3  # Level 3 for ITEM #A
                else:
                    return True, 2  # Level 2 for ITEM #
        return False, 0

    def _extract_toc_from_docling(self,
                                  doc: HintType) -> List[Tuple[str, int, str]]:
        """
        Internal fallback: try finding TOC items in the parsed DoclingDocument structure.
        (This is the method that likely failed with iXBRL before). Uses corrected ITEM_LINE_RE.
        """
        toc: List[Tuple[str, int, str]] = []
        if not doc or not hasattr(doc, 'iterate_items') or not callable(
                doc.iterate_items):
            return toc

        logger.debug(
            "Attempting internal TOC extraction from DoclingDocument items...")
        processed_titles = set(
        )  # Avoid duplicate titles from adjacent identical items
        for item, _ in doc.iterate_items():
            text = getattr(item, "text", "").strip()
            if not text: continue

            match = self.ITEM_LINE_RE.match(text)
            if match:
                is_toc, lvl = self._toc_regex(
                    text)  # Should always be true if match succeeded
                if is_toc:
                    identifier_group = match.group(1)
                    title_group = match.group('title')

                    if identifier_group and title_group:
                        title = title_group.strip()
                        if not title:
                            continue  # Skip if title extraction failed somehow

                        sec_id = sanitize_for_section_id(identifier_group)

                        # Avoid adding identical titles consecutively
                        if title not in processed_titles:
                            toc.append((title, lvl, sec_id))
                            processed_titles.add(title)
                        else:
                            logger.debug(
                                f"Skipping duplicate title found internally: '{title}'"
                            )
                    else:
                        logger.warning(
                            f"Internal TOC Regex matched but failed to capture groups for text: '{text[:100]}...'"
                        )
            else:
                processed_titles.clear(
                )  # Reset title tracking if line doesn't match TOC format

        if toc:
            logger.debug(
                f"Internal extractor found {len(toc)} potential headings.")
        else:
            logger.debug(
                "Internal extractor found no PART/ITEM headings in Docling items."
            )
        return toc

    # ------------------------------------------------------------------
    #  Main entry
    # ------------------------------------------------------------------
    def build_tree(
        self, doc: HintType, doc_meta: Dict[str, Any]
    ) -> Tuple[List[FinLensNode], Optional[FinLensNode]]:
        """
        Build a FinLensNode tree.

        1.  Check if a pre-extracted TOC is provided in `doc_meta['toc']`.
        2.  If yes, use it to build the hierarchy.
        3.  If no, try internal extraction from the `DoclingDocument` (`_extract_toc_from_docling`).
        4.  If internal extraction also fails, fall back to `HierarchicalNodeBuilder`.
        5.  Apply safety net guarantees.
        """
        self.node_counter = 0
        nodes: List[FinLensNode] = []
        root: Optional[FinLensNode] = None

        # --- Basic Doc Checks ---
        if not DOCLING_AVAILABLE or not _RuntimeDoclingDocument or not isinstance(
                doc, _RuntimeDoclingDocument):
            logger.error("Docling types unavailable or invalid doc object.")
            return [], None
        if not hasattr(doc, "iterate_items") or not callable(
                doc.iterate_items):
            logger.error("Doc object missing 'iterate_items'.")
            return [], None
        # --- End Basic Doc Checks ---

        doc_id = doc_meta.get("accession_number", "unknown_doc")
        logger.info(f"Starting TOC-Builder for document: {doc_id}")

        # --- Get TOC: Prioritize external, then internal, then fallback ---
        toc: List[Tuple[str, int,
                        str]] = doc_meta.get('toc', [])  # Check metadata first

        if toc:
            logger.info(f"Using {len(toc)} TOC entries provided via metadata.")
        else:
            logger.info(
                "No external TOC provided via metadata. Attempting internal extraction..."
            )
            toc = self._extract_toc_from_docling(
                doc)  # Try internal method as fallback
            if not toc:
                # Fallback to HierarchicalNodeBuilder if BOTH external and internal fail
                logger.warning(
                    f"Internal TOC extraction also failed for {doc_id}; falling back completely to HierarchicalNodeBuilder."
                )
                try:
                    # Ensure HierarchicalNodeBuilder is imported correctly at the top
                    nodes, root = HierarchicalNodeBuilder().build_tree(
                        doc, doc_meta)
                    return nodes, root  # Fallback builder handles safety net
                except ImportError:
                    logger.error("Fallback HierarchicalNodeBuilder not found.")
                    return [], None
                except Exception as fallback_e:
                    logger.error(
                        f"Error during fallback to HierarchicalNodeBuilder: {fallback_e}",
                        exc_info=True)
                    return [], None
            else:
                logger.info(
                    f"Using {len(toc)} TOC entries found via internal extraction."
                )

        # --- TOC Path (Using either external or internal TOC list) ---
        logger.info(f"Building TOC-based tree using {len(toc)} entries.")

        # Create Root Node
        try:
            root = FinLensNode(node_id=self._generate_node_id(),
                               parent_node_id=None,
                               doc_source_id=doc_id,
                               node_type="DOCUMENT",
                               level=0,
                               title=doc_meta.get("name", doc_id),
                               section_id="root",
                               **doc_meta)
            nodes.append(root)
        except ValidationError as e:
            logger.error(f"Root node validation failed for {doc_id}: {e}")
            return [], None
        except Exception as root_e:
            logger.error(
                f"Unexpected error creating root node for {doc_id}: {root_e}",
                exc_info=True)
            return [], None

        # Create HEADING nodes from TOC
        last_at_level: Dict[int, FinLensNode] = {0: root}
        heading_nodes_map: Dict[str, FinLensNode] = {
        }  # Map title to node for content association
        if not root:  # Should not happen if previous block succeeded, but check anyway
            logger.error(
                f"Root node is None before creating heading nodes for {doc_id}."
            )
            return [], None

        for toc_idx, (toc_title, toc_level, toc_sec_id) in enumerate(toc):
            if not toc_title or not toc_sec_id:
                logger.warning(
                    f"Skipping invalid TOC entry at index {toc_idx}: Title='{toc_title}', SecID='{toc_sec_id}'"
                )
                continue
            clamped_level = min(max(1, toc_level),
                                self.MAX_LEVEL)  # Ensure level is at least 1
            parent_level = clamped_level - 1
            parent_node = root  # Default to root if no suitable parent found
            while parent_level >= 0:
                if parent_level in last_at_level:
                    parent_node = last_at_level[parent_level]
                    break
                parent_level -= 1

            try:
                heading_node = FinLensNode(node_id=self._generate_node_id(),
                                           parent_node_id=parent_node.node_id,
                                           doc_source_id=doc_id,
                                           node_type="HEADING",
                                           level=clamped_level,
                                           title=toc_title,
                                           section_id=toc_sec_id,
                                           **doc_meta)
                nodes.append(heading_node)
                last_at_level[clamped_level] = heading_node
                # Use stripped title as key
                heading_nodes_map[toc_title.strip()] = heading_node
            except ValidationError as e:
                logger.error(
                    f"Heading node validation failed for '{toc_title}' (SecID: {toc_sec_id}, Level: {clamped_level}) in {doc_id}: {e}"
                )
            except Exception as heading_e:
                logger.error(
                    f"Unexpected error creating heading node '{toc_title}': {heading_e}",
                    exc_info=True)

        # Attach TEXT/TABLE content
        current_heading = root
        logger.debug(
            f"Starting content attachment pass for {doc_id}. Initial heading: Node {current_heading.node_id} ('{current_heading.title}')"
        )
        # <<< START DEBUG LOGGING BLOCK >>>
        for item_idx, (item, _) in enumerate(doc.iterate_items()):
            item_label = getattr(item, 'label', None)
            # IMPORTANT: Still relies on docling extracting text correctly for content nodes.
            # If docling fails on iXBRL text, content nodes might be empty/missing.
            item_text = getattr(item, "text", "").strip()
            item_id_str = str(getattr(item, "id",
                                      f"item_idx_{item_idx}"))  # Fallback ID
            item_label_str = str(item_label)  # Ensure string representation

            # --- Detailed Item Logging ---
            logger.debug(f"--- Processing Docling Item {item_idx} ---")
            logger.debug(f"  Item ID: {item_id_str}")
            logger.debug(f"  Item Label: {item_label_str}")
            logger.debug(f"  Item Text Snippet: '{item_text[:150]}...'")
            logger.debug(
                f"  Current Heading Context: Node {current_heading.node_id} ('{current_heading.title}') Section {current_heading.section_id}"
            )
            # --- End Detailed Item Logging ---

            # Update current_heading if this item IS one of our TOC headings
            # Check if the *stripped text* matches a heading title key
            heading_match_found = False  # Flag for logging
            normalized_item_text = item_text.strip()
            if normalized_item_text in heading_nodes_map:
                heading_match_found = True
                old_heading_id = current_heading.node_id
                current_heading = heading_nodes_map[normalized_item_text]
                logger.debug(
                    f"  ACTION: Matched heading title '{normalized_item_text}'. Updated current heading from {old_heading_id} to {current_heading.node_id} ('{current_heading.title}')"
                )
                # Optionally add docling ID to heading node
                # if hasattr(current_heading, 'docling_element_ids'):
                #    current_heading.docling_element_ids.append(item_id_str)
                continue  # Don't create content node for the heading itself
            else:
                # Only log if it wasn't a heading match
                logger.debug(
                    f"  INFO: Item text did not match any TOC heading titles.")

            # Attach Content Nodes (TEXT, TABLE, LIST_ITEM etc.)
            node_type = None
            text_content = None
            table_data_md = None
            if _RuntimeTableItem and isinstance(item, _RuntimeTableItem):
                node_type = "TABLE"
                if _RuntimeTableData and _RuntimeTableCell:
                    table_data_md = format_table_data_to_markdown(
                        getattr(item, "data", None))
                else:
                    logger.warning(
                        "TableData/TableCell types not available for table markdown generation."
                    )
                text_content = None  # Tables don't have primary text content here
                logger.debug(f"  INFO: Item identified as TABLE.")
            elif _RuntimeDocItemLabel and item_label in (
                    _RuntimeDocItemLabel.PARAGRAPH, _RuntimeDocItemLabel.TEXT,
                    _RuntimeDocItemLabel.LIST_ITEM):
                if item_text:  # Only create node if docling extracted text
                    node_type = "TEXT" if item_label != _RuntimeDocItemLabel.LIST_ITEM else "LIST_ITEM"
                    text_content = item_text
                    logger.debug(
                        f"  INFO: Item identified as {node_type} with text.")
                else:
                    logger.debug(
                        f"  SKIP: Item is {item_label_str} but has no text content."
                    )
                    continue  # Skip empty items
            else:
                logger.debug(
                    f"  SKIP: Item type '{item_label_str}' not processed for content node."
                )
                continue  # Skip other item types

            if node_type:
                # --- Log Node Creation Attempt ---
                logger.debug(f"  Attempting to create content node:")
                logger.debug(f"    Type: {node_type}")
                logger.debug(f"    Parent Node ID: {current_heading.node_id}")
                logger.debug(
                    f"    Parent Section ID: {current_heading.section_id}")
                logger.debug(
                    f"    Text Content Snippet: {'N/A' if text_content is None else text_content[:100] + '...'}"
                )
                logger.debug(
                    f"    Table Data Snippet: {'N/A' if table_data_md is None else table_data_md[:100] + '...'}"
                )
                logger.debug(
                    f"    Docling Element ID: {item_id_str if item_id_str else 'N/A'}"
                )
                # --- End Log Node Creation Attempt ---
                try:
                    content_node = FinLensNode(
                        node_id=self._generate_node_id(),
                        parent_node_id=current_heading.node_id,
                        doc_source_id=doc_id,
                        node_type=node_type,
                        level=min(current_heading.level + 1, self.MAX_LEVEL),
                        title=
                        None,  # Content nodes typically don't have titles unless derived
                        section_id=current_heading.
                        section_id,  # Inherit from parent heading
                        text_content=text_content,
                        table_data=table_data_md,
                        docling_element_ids=[item_id_str]
                        if item_id_str else [],
                        **doc_meta
                    )  # Pass metadata down if needed, careful about size/relevance
                    nodes.append(content_node)
                    # --- Log Success ---
                    logger.debug(
                        f"  SUCCESS: Created and appended {node_type} node {content_node.node_id}"
                    )
                    # --- End Log Success ---
                except ValidationError as e:
                    logger.error(
                        f"Content node validation failed for Docling ID {item_id_str} near '{item_text[:50]}...' in {doc_id}: {e}"
                    )
                except Exception as content_e:
                    logger.error(
                        f"Unexpected error creating content node for Docling ID {item_id_str}: {content_e}",
                        exc_info=True)

        # <<< END DEBUG LOGGING BLOCK >>>

        # Safety Net (same as before, but check root existence)
        if root:
            has_heading = any(n.node_type == "HEADING" for n in nodes)
            has_content = any(n.node_type in ("TEXT", "TABLE", "LIST_ITEM")
                              for n in nodes)

            # Ensure root is not the only node if content/headings were expected
            if len(nodes) <= 1 and toc:
                logger.warning(
                    f"Only root node exists for {doc_id} despite having {len(toc)} TOC entries. Check content attachment loop."
                )

            if not has_heading and toc:  # Only add dummy heading if TOC was expected but no headings created
                logger.warning(
                    f"No HEADING nodes created for {doc_id} via TOC path despite having TOC entries, adding dummy."
                )
                try:
                    dummy_heading = FinLensNode(
                        node_id=self._generate_node_id(),
                        parent_node_id=root.node_id,
                        doc_source_id=doc_id,
                        node_type="HEADING",
                        level=1,
                        title="Document Contents",
                        section_id="content_dummy_heading",  # More specific ID
                        **doc_meta)
                    nodes.append(dummy_heading)
                except ValidationError as e:
                    logger.error(
                        f"Dummy heading validation failed for {doc_id}: {e}")
                except Exception as dummy_h_e:
                    logger.error(
                        f"Unexpected error creating dummy heading for {doc_id}: {dummy_h_e}",
                        exc_info=True)

            if not has_content and len(
                    nodes
            ) > 1:  # Only add dummy content if headings exist but no content was attached
                logger.warning(
                    f"No TEXT/TABLE/LIST_ITEM nodes created for {doc_id} via TOC path, adding dummy."
                )
                # Find first heading or fall back to root
                parent_for_dummy = next(
                    (n for n in nodes if n.node_type == 'HEADING'), root)
                try:
                    dummy_content = FinLensNode(
                        node_id=self._generate_node_id(),
                        parent_node_id=parent_for_dummy.node_id,
                        doc_source_id=doc_id,
                        node_type="TEXT",
                        level=parent_for_dummy.level + 1,
                        title=None,
                        section_id=parent_for_dummy.section_id,
                        text_content=
                        "[No content found or attached]",  # Clearer message
                        **doc_meta)
                    nodes.append(dummy_content)
                except ValidationError as e:
                    logger.error(
                        f"Dummy content validation failed for {doc_id}: {e}")
                except Exception as dummy_c_e:
                    logger.error(
                        f"Unexpected error creating dummy content for {doc_id}: {dummy_c_e}",
                        exc_info=True)

        else:
            logger.error(
                f"Root node is None for {doc_id} after TOC processing. Cannot apply safety net."
            )

        logger.info(
            f"Built TOC-based tree with {len(nodes)} nodes for {doc_id}.")
        return nodes, root
