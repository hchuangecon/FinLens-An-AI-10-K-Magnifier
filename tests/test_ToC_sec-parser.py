import re
from typing import List, Optional, Dict, Any


# _parse_markdown_table_cells remains the same
def _parse_markdown_table_cells(markdown_table_string: str) -> List[List[str]]:
    lines = markdown_table_string.strip().split('\n')
    parsed_rows: List[List[str]] = []
    for line in lines:
        line_s = line.strip()
        if not line_s.startswith("|") or not line_s.endswith("|"):
            continue

        temp_cells_for_sep_check = line_s[1:-1].split('|')
        if not temp_cells_for_sep_check:
            is_separator = False
        else:
            is_separator = True
            has_hyphen = False
            for cell_content in temp_cells_for_sep_check:
                stripped_cell = cell_content.strip()
                if not all(c in '-:' for c in stripped_cell):
                    is_separator = False
                    break
                if '-' in stripped_cell:
                    has_hyphen = True
            if not has_hyphen:
                is_separator = False

        if is_separator:
            continue

        cells = [cell.strip() for cell in temp_cells_for_sep_check]
        parsed_rows.append(cells)
    return parsed_rows


# UPDATED _is_page_number to avoid treating years as appended numbers
def _is_page_number(text: str) -> Optional[str]:
    """
    Checks if the text is a page number. Returns the page number string or None.
    Priority: Standalone known formats, then appended numbers (with caution).
    """
    text_cleaned = text.strip()

    # Priority 1: Standalone, well-defined formats (full match)
    match_arabic = re.fullmatch(r"(\d{1,4})", text_cleaned)
    if match_arabic:
        # Added check: Don't treat 4-digit numbers like years as page numbers if standalone
        # This is a policy choice - usually page numbers don't exceed ~1500.
        # If the number looks like a common year, maybe reject?
        # Let's keep it simple: if it's 19xx or 20xx maybe it's less likely a page number?
        # For now, let's accept any 1-4 digits as standalone page numbers.
        return match_arabic.group(1)

    match_appendix_combined = re.fullmatch(r"([A-Za-z])(?:-|\s*)(\d{1,3})",
                                           text_cleaned)
    if match_appendix_combined:
        return f"{match_appendix_combined.group(1).upper()}-{match_appendix_combined.group(2)}"

    match_appendix_simple = re.fullmatch(r"([A-Za-z])(\d{1,3})", text_cleaned)
    if match_appendix_simple:
        return f"{match_appendix_simple.group(1).upper()}-{match_appendix_simple.group(2)}"

    match_roman = re.fullmatch(r"([IVXLCDM]{1,4})", text_cleaned,
                               re.IGNORECASE)
    if match_roman:
        return match_roman.group(1).upper()

    # Priority 2: Number appended to text (if not a full match above)
    appended_match = re.match(r"^(.*[^0-9\s])\s*(\d{1,4})$", text_cleaned)
    if appended_match:
        preceding_text = appended_match.group(1).strip()
        extracted_num = appended_match.group(2)

        # --- NEW Check: Avoid treating years like 19xx/20xx as appended page numbers ---
        if len(extracted_num) == 4 and extracted_num.startswith(('19', '20')):
            # If the number looks like a year, DON'T treat it as an appended page number.
            return None

        # Check for common phrases that mean "reference to a page"
        if len(preceding_text.split()) > 1 and \
           re.search(r"\b(page|pg|p|see|ref|table|figure|note|item)\b", preceding_text, re.IGNORECASE):
            return None

        # Original condition for valid appended number (check preceding text)
        if len(preceding_text) > 1 or \
           (len(preceding_text) == 1 and not (preceding_text.isalpha() and re.fullmatch(r"[IVXLCDM]", preceding_text, re.IGNORECASE))):
            if len(extracted_num) > 0:
                return extracted_num

    return None


# _is_potential_title_text remains the same as the version you just tested
# (This version correctly rejected "Alice", "New York", "Value 1" in the debug output)
def _is_potential_title_text(text: str) -> bool:
    """
    Heuristically checks if a text string could be part of a report title or company name.
    (Implementation is the same as the version you just tested)
    """
    text_s = text.strip()
    # print(f"DEBUG: Checking title text: '{text_s}'") # Optional debug print

    # 1. Basic initial filters
    if not (2 < len(text_s) < 150):
        if len(text_s) == 3 and text_s.lower() not in [
                "inc", "co.", "ltd", "llc", "llp", "form", "sec"
        ]:
            return False
        elif len(text_s) <= 2:
            return False
        if len(text_s) >= 150: return False

    # Use the UPDATED _is_page_number here. Now ".../2024" should return None.
    if _is_page_number(text_s):
        return False

    if not re.search(r"[a-zA-Z]", text_s):
        return False

    if text_s.isdigit():
        return False

    text_lower = text_s.lower()

    # 2. Strong Indicators (Keywords, Year, Separators, Form/Item patterns)
    title_keywords = [
        "inc", "corp", "llc", "ltd", "company", "form", "report", "annual",
        "co.", "quarterly", "appendix", "exhibit", "schedule", "statements",
        "sec", "financial", "notes", "contents", "index", "summary",
        "consolidated", "incorporated", "limited", "partnership"
    ]
    for keyword in title_keywords:
        safe_keyword = re.escape(keyword.replace('.', r'\.'))
        if re.search(r"\b" + safe_keyword + r"\b", text_lower, re.IGNORECASE):
            # print(f"DEBUG: Matched Keyword (boundary) '{keyword}' for '{text_s}'")
            return True
        if keyword.endswith('.') and keyword in text_lower:
            # print(f"DEBUG: Matched Keyword (contains) '{keyword}' for '{text_s}'")
            return True

    if re.search(r"\b(19\d{2}|20\d{2})\b", text_s) and re.search(
            r"[a-zA-Z]", text_s):
        # print(f"DEBUG: Matched Year for '{text_s}'")
        return True  # "JPMorgan.../2024" should now hit this if _is_page_number is fixed.

    if re.search(r"[/\-&]", text_s) and re.search(r"[a-zA-Z]", text_s) and \
       (len(text_s.split(None, 1)) > 1 or len(text_s) > 5):
        # print(f"DEBUG: Matched Separator for '{text_s}'")
        return True  # "JPMorgan.../2024" should also hit this.

    if re.search(r"\b(Form|Item|Part|Section|Schedule|Exhibit|Appendix|Note)\s+([A-Za-z0-9\-.]+)\b", text_s, re.IGNORECASE) or \
       re.search(r"\b([A-Za-z0-9\-.]+)\s+(Form|Item|Part|Section|Schedule|Exhibit|Appendix|Note)\b", text_s, re.IGNORECASE):
        # print(f"DEBUG: Matched Form/Item/Part for '{text_s}'")
        return True

    # 3. Capitalization and structure (Fallback)
    words = re.split(r'[\s/\-]+', text_s)
    words = [w for w in words if w]

    if len(words) >= 2:
        cap_words = sum(1 for word in words if word and word[0].isupper())
        if len(words) == 2 and cap_words == 2 and all(w.isalpha()
                                                      for w in words):
            # print(f"DEBUG: Simple proper noun rejected for '{text_s}'")
            return False
        if cap_words / len(words) >= 0.4 or cap_words >= 3:
            # print(f"DEBUG: Matched Capitalization for '{text_s}'")
            return True

    if len(words) == 1 and text_s[0].isupper() and len(
            text_s) > 5 and text_s.lower() not in [
                "value", "name", "total", "assets", "amount"
            ]:
        # print(f"DEBUG: Matched Single long capitalized word for '{text_s}'")
        return True

    # print(f"DEBUG: No title match for '{text_s}'")
    return False


# find_page_table_pattern remains the same as the version you just tested
def find_page_table_pattern(markdown_table_string: str) -> Dict[str, Any]:
    rows_of_cells = _parse_markdown_table_cells(markdown_table_string)

    if not rows_of_cells:
        return {
            'is_page_table': False,
            'page_number': None,
            'pattern_type': None
        }

    for row_cells in rows_of_cells:
        if not any(cell.strip() for cell in row_cells):
            continue

        page_number_in_current_row: Optional[str] = None
        page_number_cell_index: int = -1
        page_number_cell_count_in_row: int = 0

        for i, cell_text in enumerate(row_cells):
            # Use the LATEST _is_page_number
            pn = _is_page_number(cell_text)
            if pn:
                page_number_cell_count_in_row += 1
                page_number_in_current_row = pn
                page_number_cell_index = i

        if page_number_cell_count_in_row == 1:
            all_other_cells_are_empty = True
            for i, cell_text in enumerate(row_cells):
                if i != page_number_cell_index and cell_text.strip():
                    all_other_cells_are_empty = False
                    break
            if all_other_cells_are_empty:
                return {
                    'is_page_table': True,
                    'page_number': page_number_in_current_row,
                    'pattern_type': "Type 1"
                }

        non_empty_cells_details = []
        for i, cell_text in enumerate(row_cells):
            if cell_text.strip():
                non_empty_cells_details.append({
                    'index': i,
                    'content': cell_text.strip()
                })

        page_number_cells_in_row = []
        title_text_cells_in_row = []

        for cell_detail in non_empty_cells_details:
            # Use the LATEST _is_page_number
            pn = _is_page_number(cell_detail['content'])
            if pn:
                page_number_cells_in_row.append({
                    'index': cell_detail['index'],
                    'number': pn
                })
            # Use the LATEST _is_potential_title_text
            elif _is_potential_title_text(cell_detail['content']):
                title_text_cells_in_row.append({
                    'index': cell_detail['index'],
                    'text': cell_detail['content']
                })

        if len(page_number_cells_in_row) == 1 and len(
                title_text_cells_in_row) >= 1:
            if not (2 <= len(non_empty_cells_details) <= 3):
                continue

            page_num_cell_info = page_number_cells_in_row[0]
            min_title_idx = min(tc['index'] for tc in title_text_cells_in_row)
            max_title_idx = max(tc['index'] for tc in title_text_cells_in_row)

            if page_num_cell_info['index'] < min_title_idx:
                return {
                    'is_page_table': True,
                    'page_number': page_num_cell_info['number'],
                    'pattern_type': "Type 3"
                }
            elif page_num_cell_info['index'] > max_title_idx:
                return {
                    'is_page_table': True,
                    'page_number': page_num_cell_info['number'],
                    'pattern_type': "Type 2"
                }

    return {'is_page_table': False, 'page_number': None, 'pattern_type': None}


# Keep the __main__ block for testing consistency
if __name__ == '__main__':
    print("--- Debugging _is_potential_title_text ---")
    test_title_string = "JPMorgan Chase & Co./2024"
    print(
        f"Testing title string: '{test_title_string}' -> {_is_potential_title_text(test_title_string)}"
    )  # Should hopefully be True now

    test_title_string_alice = "Alice"
    print(
        f"Testing title string: '{test_title_string_alice}' -> {_is_potential_title_text(test_title_string_alice)}"
    )  # Should be False

    test_title_string_new_york = "New York"
    print(
        f"Testing title string: '{test_title_string_new_york}' -> {_is_potential_title_text(test_title_string_new_york)}"
    )  # Should be False

    test_title_string_value1 = "Value 1"
    print(
        f"Testing title string: '{test_title_string_value1}' -> {_is_potential_title_text(test_title_string_value1)}"
    )  # Should be False

    print("\n--- Full Table Tests ---")
    test_markdown_tables = [
        # Type 1 Examples
        "| 42 |",
        "|    | 43 |    |",
        "| IV |",
        "| C-1 |",
        "| A1 |",
        "| B-2 |",
        "| JPMorgan Chase & Co./2024 Form 10-K51 |",
        "| ReportName123 |",
        "| Text with space 12 |",
        # Type 2 Examples
        "| JPMorgan Chase & Co./2024 Form 10-K |   | 49 |",
        "| Some Report Title Inc. | | 107 |",
        "| Report Section A | Content | 23 |",
        "| Tesla, Inc. Annual Report | | vi |",
        "| Apple Inc. | 2023 Form 10-K | 5 |",
        "| Apple Inc. | Report Details | Page 55 |",
        "| Notes to Financials | Report2023p5 |",
        # Type 3 Examples
        "| 50 |   | JPMorgan Chase & Co./2024 Form 10-K |",
        "| 108 | | Some Other Report Title LLC |",
        "| 24 | Content | Report Section B |",
        "| vii | | Tesla, Inc. Annual Report |",
        # Negative Examples
        "| Name | Age | City |",
        "|------|-----|------|",
        "| Alice | 30 | New York |",
        "| Just some random text in a cell |",
        "| 1 | 2 | 3 |",
        "| Company A | Value 1 | 100 |",
        "| |",
        "Text outside table",
        "| This is some text | and this is some other text |",
        "| 100 | 200 |",
        "| Text | 100 | Text2 | 200 |",
        "| Financial Statement | Note | See Page 23 |",
        "| IX - Appendix |",
        "| Item | Description | Value |",
        "| C-1D |",
        "| Report12345 |",
        "| 49 |  | JPMorgan Chase & Co./2024 Form 10-K |",
    ]

    for i, md_table in enumerate(test_markdown_tables):
        print(f"--- Testing Table {i+1}: {md_table.splitlines()[0][:70]} ---")
        result = find_page_table_pattern(md_table)
        print(f"Result: {result}\n")

    table_with_header_str = """
| Document Reference          |      | Page Number |
|-----------------------------|------|-------------|
| JPMorgan Chase & Co./2024   |      | 123         |
    """
    print(f"--- Testing Table with Header ---")
    result = find_page_table_pattern(table_with_header_str)
    print(f"Result: {result}\n")  # Hopefully True now
