# test/test2_toc_parser.py (or wherever your test utilities are)

import sys
import os  # Added import os
from pathlib import Path
import logging
import re
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from pydantic import ValidationError, BaseModel, Field, ConfigDict
from src.config.settings import get_settings
import requests

# --- Add src directory to sys.path FIRST ---
project_root = Path(
    __file__).resolve().parent.parent  # Assumes script is in tests/
src_path = project_root / 'src'
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))
    print(f"--- Prepended src path: {src_path} ---")
else:
    print(f"--- src path already in sys.path: {src_path} ---")

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Suppress XML-as-HTML warnings from BeautifulSoup
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# --- Utilities ---


def download_10k_html(cik: str, accession_number: str,
                      primary_doc_filename: str, save_as: str):
    settings = get_settings()
    edgar_base = settings.sec_api.edgar_archive_base
    url = f"{edgar_base}/{int(cik)}/{accession_number.replace('-', '')}/{primary_doc_filename}"

    dest_folder = Path(__file__).parent / "fixtures"
    dest_folder.mkdir(exist_ok=True)
    dest_file = dest_folder / f"{save_as}.html"

    print(f"Downloading from {url}...")
    response = requests.get(
        url, headers={"User-Agent": settings.sec_api.user_agent})
    response.raise_for_status()
    dest_file.write_text(response.text, encoding='utf-8')
    print(f"Saved to {dest_file}")


def load_html(name: str) -> str:
    base_path = Path(__file__).parent
    fixtures_dir = base_path / 'fixtures'
    path_htm = fixtures_dir / (name + '.htm')
    path_html = fixtures_dir / (name + '.html')
    path_to_use = path_htm if path_htm.is_file(
    ) else path_html if path_html.is_file() else None
    if path_to_use is None:
        raise FileNotFoundError(
            f"HTML fixture '{name}.(htm|html)' not found in {fixtures_dir}")
    return path_to_use.read_text(encoding='utf-8')


def load_docling(name: str) -> Optional[Any]:
    html = load_html(name)
    try:
        from docling.document_converter import DocumentConverter, FormatOption
        from docling.datamodel.base_models import InputFormat, DocumentStream
        from docling.pipeline.simple_pipeline import SimplePipeline
        from docling.backend.html_backend import HTMLDocumentBackend
        import io
    except ImportError:
        logger.error("Docling components not available, skipping parsing.")
        return None

    fmt_opt = FormatOption(backend=HTMLDocumentBackend,
                           pipeline_cls=SimplePipeline)
    converter = DocumentConverter(allowed_formats=[InputFormat.HTML],
                                  format_options={InputFormat.HTML: fmt_opt})
    html_stream = io.BytesIO(html.encode('utf-8'))
    stream = DocumentStream(name=f"{name}.html", stream=html_stream)
    result = converter.convert(source=stream, raises_on_error=False)
    status = getattr(getattr(result, 'status', None), 'name', '')
    if status in ('SUCCESS', 'PARTIAL_SUCCESS') and result.document:
        doc = result.document
        doc.__dict__['source_html'] = html
        return doc
    logger.error(f"Docling parse failed (status={status}).")
    return None


class ToCExtractor:
    """
    Extracts a table of contents by scanning text lines, HTML anchors,
    table cells, and common block tags for "ITEM X" and "PART I" headings.
    """
    ITEM_LINE_RE = re.compile(
        r'(?i)^\s*(ITEM\s+\d+[A-Z]?|PART\s+[IVX]+)[\.: \-\u2013\u2014]+(.+?)\s*$',
        re.MULTILINE)

    def extract_from_html(self, html: str) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, 'html.parser')
        candidates: List[Dict[str, Any]] = []

        # 1) Scan rendered text lines for ITEM/PART headings
        for idx, line in enumerate(soup.get_text().splitlines()):
            m = self.ITEM_LINE_RE.match(line)
            if m:
                sect = m.group(1).upper().replace(' ', '_')
                title = m.group(2).strip()
                candidates.append({'id': sect, 'title': title, 'offset': idx})

        # 2) Scan <a name=> and <a id=> anchors
        anchors = (
            soup.find_all('a', attrs={'name': re.compile(r'ITEM|PART', re.I)})
            + soup.find_all('a', attrs={'id': re.compile(r'ITEM|PART', re.I)}))
        for a in anchors:
            slug = (a.get('name') or a.get('id') or '').upper()
            if not slug:
                continue
            sib = a.find_next_sibling(['b', 'strong', 'span'])
            title = sib.get_text(strip=True) if sib else slug
            offset = getattr(a, 'sourceline', 0) or 0
            candidates.append({'id': slug, 'title': title, 'offset': offset})

        # 3) Scan <td>, <th>, <p>, <div>, <span> for ITEM/PART headings
        for tag in soup.find_all(['td', 'th', 'p', 'div', 'span']):
            text = tag.get_text(strip=True)
            m = self.ITEM_LINE_RE.match(text)
            if m:
                sect = m.group(1).upper().replace(' ', '_')
                title = m.group(2).strip()
                offset = getattr(tag, 'sourceline', 0) or 0
                candidates.append({
                    'id': sect,
                    'title': title,
                    'offset': offset
                })

        # 4) De-duplicate by id, keeping the earliest offset
        best: Dict[str, Dict[str, Any]] = {}
        for c in candidates:
            if c['id'] not in best or c['offset'] < best[c['id']]['offset']:
                best[c['id']] = c

        # 5) Return sorted by appearance
        return sorted(best.values(), key=lambda x: x['offset'])


# --- Example usage ---
if __name__ == "__main__":
    print("\n--- Running Real 10-K Test ---")
    try:
        download_10k_html(cik="320193",
                          accession_number="0000320193-24-000123",
                          primary_doc_filename="aapl-20240928.htm",
                          save_as="aapl-20240928_10k")

        doc = load_docling('aapl-20240928_10k')
        assert doc is not None, "Failed to parse 10-K"
        assert hasattr(doc, 'source_html'), "No source_html attached"
        print("DoclingDocument loaded and parsed successfully.")

        toc_extractor = ToCExtractor()
        toc_entries = toc_extractor.extract_from_html(doc.source_html)
        print(f"Extracted {len(toc_entries)} ToC entries.")
        for entry in toc_entries[:20]:
            print(entry)

    except Exception as e:
        print(f"Test failed: {e}")
