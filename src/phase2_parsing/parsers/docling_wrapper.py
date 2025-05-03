# src/phase2_parsing/parsers/docling_wrapper.py
import logging
import io  # Import the io module
import hashlib  # Import hashlib for hashing
from pathlib import Path
from typing import Optional, Any, Type, TYPE_CHECKING, Union  # Add Union

# --- Define Availability Flag First ---
DOCLING_AVAILABLE = True
ESSENTIAL_TYPES_AVAILABLE = True  # Flag for essential runtime types

# --- Conditional Import for Static Type Checking ---
if TYPE_CHECKING:
    from docling.document_converter import DocumentConverter, FormatOption
    # Direct import for type checking
    from docling.datamodel.base_models import InputFormat, DocumentStream, ConversionStatus
    # Import InputDocument for type checking
    from docling.datamodel.document import DoclingDocument, ConversionResult, InputDocument
    from docling.pipeline.base_pipeline import BasePipeline
    from docling.pipeline.simple_pipeline import SimplePipeline
    from docling.datamodel.pipeline_options import PipelineOptions
    from docling.backend.abstract_backend import AbstractDocumentBackend
    from docling.backend.html_backend import HTMLDocumentBackend
    # Import the CORRECT V2 backend class
    from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
    from docling_core.types.doc import TableItem, DocItemLabel

# --- End Conditional Import ---

try:
    # Import essential classes needed at runtime
    from docling.document_converter import DocumentConverter, FormatOption
    # --- Direct Import ---
    from docling.datamodel.base_models import InputFormat  # Import directly
    # --- End Direct Import ---
    from docling.datamodel.base_models import DocumentStream, ConversionStatus
    # --- Import InputDocument ---
    from docling.datamodel.document import DoclingDocument, ConversionResult, InputDocument
    # --- End Import InputDocument ---
    from docling.pipeline.base_pipeline import BasePipeline
    from docling.pipeline.simple_pipeline import SimplePipeline
    from docling.datamodel.pipeline_options import PipelineOptions
    from docling.backend.abstract_backend import AbstractDocumentBackend
    from docling.backend.html_backend import HTMLDocumentBackend
    # --- Import the CORRECT V2 backend CLASS ---
    from docling.backend.docling_parse_v2_backend import DoclingParseV2DocumentBackend
    # --- End Import ---
    from docling_core.types.doc import TableItem, DocItemLabel

    logging.info("Successfully imported Docling library components.")

except ImportError as e:
    DOCLING_AVAILABLE = False
    logging.error(
        f"Docling library or core types not found: {e}. DoclingWrapper may be non-functional."
    )

# --- Check Essential Types After Import Attempt ---
# Use InputFormat (direct import name) for the check here
essential_runtime_classes = [
    DocumentConverter,
    FormatOption,
    InputFormat,  # Check the direct import name
    DocumentStream,
    ConversionStatus,
    DoclingDocument,
    InputDocument,  # Check InputDocument
    BasePipeline,
    SimplePipeline,
    PipelineOptions,
    AbstractDocumentBackend,
    HTMLDocumentBackend
]
if not all(essential_runtime_classes):
    ESSENTIAL_TYPES_AVAILABLE = False
    missing = [
        cls_obj.__name__ if hasattr(cls_obj, '__name__') else str(cls_obj)
        for cls_obj in essential_runtime_classes if cls_obj is None
    ]
    logging.critical(
        f"Essential Docling runtime types missing: {missing}. Wrapper cannot function correctly."
    )
    DOCLING_AVAILABLE = False

if not all([TableItem, DocItemLabel]):
    logging.warning(
        "Docling core types (TableItem, DocItemLabel) missing. Output validation will be skipped."
    )

# --- End Runtime Import and Check ---

logger = logging.getLogger(__name__)


class DoclingWrapper:
    """Wraps the Docling DocumentConverter for use in the pipeline."""

    def __init__(self):
        """Initializes the Docling converter if available."""
        self.converter: Optional['DocumentConverter'] = None
        if DOCLING_AVAILABLE and ESSENTIAL_TYPES_AVAILABLE and DocumentConverter:
            try:
                # Initialize the main converter. Backend instances will be created per parse call if needed.
                self.converter = DocumentConverter()
                logger.info(
                    "DoclingWrapper initialized with DocumentConverter.")
            except Exception as e:
                logger.error(
                    f"Failed to initialize Docling DocumentConverter: {e}",
                    exc_info=True)
                self.converter = None
        else:
            logger.error(
                "Docling is not available or essential types/DocumentConverter class not found. Wrapper cannot be initialized."
            )
            self.converter = None

    def parse(
        self,
        input_path:
        Path,  # Expecting path to actual file to be processed (HTML or PDF)
        pipeline_cls: Optional[Type['BasePipeline']] = None,
        backend_cls: Optional[
            Type['AbstractDocumentBackend']] = None  # The CLASS to use
    ) -> Optional['DoclingDocument']:
        """
        Parses a document using Docling, using the specified backend class.
        """
        # Check essential types availability using the direct import name
        if not self.converter or not ESSENTIAL_TYPES_AVAILABLE or not InputFormat or not InputDocument:
            logger.error(
                "Docling converter not initialized or InputFormat/InputDocument unavailable at runtime."
            )
            return None

        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            return None

        logger.info(f"Starting Docling parse for: {input_path.name}")

        # Determine Input Format based on suffix of the file being processed
        input_fmt: Optional[InputFormat] = None  # Initialize as None
        try:
            suffix = input_path.suffix.lower()
            if suffix in ['.html', '.htm']:
                input_fmt = InputFormat.HTML
            elif suffix == '.pdf':
                input_fmt = InputFormat.PDF
            # Add other elif branches for supported InputFormat members as needed
            # e.g., elif suffix == '.docx': input_fmt = InputFormat.DOCX
        except AttributeError:
            # This would only happen if InputFormat itself is None or not an Enum
            logger.critical(
                "InputFormat Enum object is not available or invalid. Cannot determine format."
            )
            return None

        if input_fmt is None:
            logger.warning(
                f"Could not determine input format for {input_path.name} based on suffix. Docling will attempt auto-detection."
            )

        # Use the backend class provided by the caller (ProcessorService)
        effective_backend_cls = backend_cls
        effective_pipeline_cls = pipeline_cls if pipeline_cls else SimplePipeline  # Default pipeline

        backend_name = getattr(effective_backend_cls, '__name__',
                               'Invalid/Missing')
        pipeline_name = getattr(effective_pipeline_cls, '__name__',
                                'Invalid/Missing')
        logger.info(
            f"Using specified Backend Class: {backend_name}, Pipeline Class: {pipeline_name}"
        )

        if not effective_backend_cls or not effective_pipeline_cls:
            logger.error(
                f"Cannot proceed with parse: Effective Backend Class ({backend_name}) or Pipeline Class ({pipeline_name}) is missing/invalid at runtime."
            )
            return None

        # --- Declare input_doc_obj here to ensure it's in scope for finally ---
        input_doc_obj: Optional[InputDocument] = None
        # ----------------------------------------------------------------------
        try:
            # Read file content into BytesIO
            file_content: Optional[bytes] = None
            try:
                with open(input_path, "rb") as f:
                    file_content = f.read()
            except Exception as read_err:
                logger.error(
                    f"Failed to read file content from {input_path}: {read_err}",
                    exc_info=True)
                return None

            if file_content is None:
                logger.error(
                    f"File content is empty or could not be read from {input_path}"
                )
                return None

            bytes_stream = io.BytesIO(file_content)

            # --- Create InputDocument Correctly ---
            try:
                # Pass stream as first arg, determined format, and the backend CLASS
                input_doc_obj = InputDocument(
                    path_or_stream=bytes_stream,  # Pass the stream
                    format=input_fmt,  # Pass determined format (can be None)
                    backend=effective_backend_cls,  # Pass the backend CLASS
                    filename=input_path.name  # Pass filename when using stream
                )
                logger.debug(
                    f"Created InputDocument for {input_path.name} with format {input_fmt}"
                )
            except Exception as input_doc_err:
                # Log the specific error from InputDocument init
                logger.error(
                    f"Failed to create InputDocument for {input_path.name}: {input_doc_err}",
                    exc_info=True)
                # Check if the error is the specific RuntimeError we saw before
                if isinstance(
                        input_doc_err, RuntimeError
                ) and "Incompatible file format" in str(input_doc_err):
                    logger.error(
                        f"Backend/Format Mismatch: Tried to use {backend_name} for format {input_fmt}"
                    )
                return None
            # --- End Create InputDocument ---

            # --- Backend Instantiation is handled by InputDocument ---

            # --- Create DocumentStream for convert() ---
            bytes_stream.seek(0)
            if not DocumentStream:
                raise RuntimeError("Docling DocumentStream not available")
            doc_stream = DocumentStream(name=input_path.name,
                                        stream=bytes_stream)
            # --- End Create DocumentStream ---

            # --- Options are not passed to convert() ---
            # ------------------------------------------

            if not ConversionStatus:
                raise RuntimeError("Docling ConversionStatus not available")

            # --- Perform the conversion WITHOUT options keyword ---
            logger.debug(
                f"Calling self.converter.convert for {input_path.name}")
            result = self.converter.convert(
                source=doc_stream,
                # options=options, # REMOVED
                raises_on_error=False)
            logger.debug(
                f"self.converter.convert call completed for {input_path.name}")
            # ----------------------------------------------------

            # Check conversion status
            success_status = getattr(ConversionStatus.SUCCESS, 'name',
                                     'SUCCESS')
            partial_success_status = getattr(ConversionStatus.PARTIAL_SUCCESS,
                                             'name', 'PARTIAL_SUCCESS')
            status = getattr(getattr(result, 'status', None), 'name',
                             'UNKNOWN')

            if status in (success_status, partial_success_status
                          ) and DoclingDocument and result.document:
                logger.info(
                    f"Docling parse successful for: {input_path.name} (Status: {status})"
                )
                if isinstance(result.document, DoclingDocument):
                    return result.document  # Backend unload handled in finally
                else:
                    logger.error(
                        f"Docling conversion returned success status but document is not DoclingDocument type for {input_path.name}"
                    )
                    return None  # Backend unload handled in finally
            else:
                logger.error(
                    f"Docling pipeline execution failed for: {input_path.name}. Status: {status}"
                )
                error_msg = getattr(result, 'message', 'No error message.')
                logger.error(f"Docling error details: {error_msg}")
                if hasattr(result, 'errors') and result.errors:
                    for err_item in result.errors:
                        logger.error(
                            f"  - Component: {getattr(err_item, 'component_type', 'N/A')}, Module: {getattr(err_item, 'module_name', 'N/A')}, Message: {getattr(err_item, 'error_message', 'N/A')}"
                        )
                return None  # Backend unload handled in finally

        except Exception as e:
            logger.error(
                f"Unexpected error during Docling processing for {input_path.name}: {e}",
                exc_info=True)
            return None  # Backend unload handled in finally
        finally:
            # --- Ensure backend is unloaded ---
            if input_doc_obj and hasattr(input_doc_obj,
                                         '_backend') and hasattr(
                                             input_doc_obj._backend, 'unload'):
                try:
                    input_doc_obj._backend.unload()
                    logger.debug(f"Unloaded backend for {input_path.name}")
                except Exception as unload_err:
                    logger.warning(
                        f"Failed to unload backend for {input_path.name}: {unload_err}"
                    )
            # --------------------------------

    def validate_parsing_output(self,
                                doc: Optional['DoclingDocument']) -> bool:
        """
        Validates the output of Docling parsing, focusing on table structure.
        """
        if not DOCLING_AVAILABLE or not doc:
            logger.debug(
                "Docling not available or no document provided, skipping validation."
            )
            return True

        RuntimeTableItem = globals().get('TableItem')
        RuntimeDocItemLabel = globals().get('DocItemLabel')

        if not RuntimeTableItem or not RuntimeDocItemLabel:
            logger.warning(
                "Docling core types (TableItem, DocItemLabel) not available at runtime. Cannot perform detailed validation."
            )
            return True

        if not hasattr(doc, 'iterate_items') or not callable(
                doc.iterate_items):
            logger.warning(
                "Provided document object does not have 'iterate_items' method. Skipping validation."
            )
            return True

        table_count = 0
        suspicious_tables = 0
        MIN_ROWS_THRESHOLD = 1
        MIN_COLS_THRESHOLD = 1

        logger.debug(
            f"Starting validation for DoclingDocument: {getattr(doc, 'name', 'Unknown')}"
        )
        try:
            items_iterator = doc.iterate_items()
            for item, _ in items_iterator:
                if isinstance(item, RuntimeTableItem):
                    table_count += 1
                    is_suspicious = False
                    table_data = getattr(item, 'data', None)
                    if table_data is None:
                        logger.warning(
                            f"TableItem found with missing 'data' attribute (ID: {getattr(item, 'id', 'N/A')})."
                        )
                        is_suspicious = True
                    elif not hasattr(table_data, 'num_rows') or not hasattr(
                            table_data, 'num_cols'):
                        logger.warning(
                            f"TableItem data object missing 'num_rows' or 'num_cols' (ID: {getattr(item, 'id', 'N/A')})."
                        )
                        is_suspicious = True
                    elif table_data.num_rows < MIN_ROWS_THRESHOLD or table_data.num_cols < MIN_COLS_THRESHOLD:
                        logger.debug(
                            f"Table found with low row/col count: {table_data.num_rows}x{table_data.num_cols} (ID: {getattr(item, 'id', 'N/A')})"
                        )

                    if is_suspicious: suspicious_tables += 1
        except Exception as e:
            logger.error(f"Error during Docling output validation: {e}",
                         exc_info=True)
            return False

        logger.debug(
            f"Validation complete. Total tables: {table_count}, Suspicious tables: {suspicious_tables}"
        )
        is_valid = suspicious_tables == 0
        if not is_valid:
            logger.warning(
                f"Docling output validation failed for {getattr(doc, 'name', 'Unknown')}: Found {suspicious_tables} suspicious tables."
            )
        return is_valid
