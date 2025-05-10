import re
import html  # For unescaping HTML entities


def count_raw_snippet_occurrences_revised(html_content: str,
                                          snippets_to_check: dict) -> dict:
    """
    Counts occurrences of specific text snippets by comparing normalized versions
    of the HTML content and the snippets.
    The search is case-insensitive.

    Args:
        html_content: The raw HTML content as a string.
        snippets_to_check: A dictionary where keys are descriptive names and 
                           values are the raw text snippets (e.g., from JSON output).

    Returns:
        A dictionary with the count of occurrences for each snippet.
    """
    results = {}

    # 1. Normalize the HTML content for searching:
    #    - Unescape HTML entities (e.g., &nbsp; -> ' ', &amp; -> '&')
    #    - Replace various whitespace characters (including &nbsp; after unescaping) with a single space
    #    - Convert to lowercase
    try:
        normalized_html_for_search = html.unescape(html_content)
        normalized_html_for_search = ' '.join(
            normalized_html_for_search.split()).lower()
    except Exception as e:
        print(f"Error normalizing HTML content: {e}")
        # If HTML normalization fails, we can't proceed with searching it.
        for name in snippets_to_check:
            results[name] = {
                "searched_prefix_normalized": "[HTML NORMALIZATION FAILED]",
                "count_in_normalized_html": -1  # Indicate error
            }
        return results

    for name, text_to_find_raw_from_json in snippets_to_check.items():
        # 2. Normalize the snippet (from JSON) in the same way.
        #    Use a substantial prefix to define the search pattern.
        prefix_length = 100  # Length of the prefix to use for matching

        try:
            # Take a prefix of the raw snippet before normalization
            snippet_prefix_raw = text_to_find_raw_from_json[:prefix_length +
                                                            50]  # Take a bit more for normalization robustness

            normalized_snippet_for_search = html.unescape(snippet_prefix_raw)
            normalized_snippet_for_search = ' '.join(
                normalized_snippet_for_search.split()).lower()
            # After normalization, then take the defined prefix length
            normalized_snippet_for_search = normalized_snippet_for_search[:
                                                                          prefix_length]

        except Exception as e:
            print(f"Error normalizing snippet '{name}': {e}")
            results[name] = {
                "searched_prefix_normalized": "[SNIPPET NORMALIZATION FAILED]",
                "count_in_normalized_html": -1  # Indicate error
            }
            continue

        count = 0
        try:
            if not normalized_snippet_for_search:  # Skip if snippet becomes empty
                results[name] = {
                    "searched_prefix_normalized":
                    normalized_snippet_for_search,
                    "count_in_normalized_html": 0
                }
                continue

            # Escape for regex
            escaped_pattern = re.escape(normalized_snippet_for_search)
            # Find all non-overlapping occurrences in the normalized HTML
            matches = re.findall(
                escaped_pattern, normalized_html_for_search
            )  # re.IGNORECASE is not needed as both are lowercased
            count = len(matches)
        except Exception as e:
            print(f"Error during regex search for snippet '{name}': {e}")
            count = -1  # Indicate error

        results[name] = {
            "searched_prefix_normalized": normalized_snippet_for_search,
            "count_in_normalized_html": count
        }
    return results


if __name__ == "__main__":
    # IMPORTANT: Ensure this path is correct and the script can access the file.
    html_file_path = "/Users/mileshuang/Desktop_Mac_Studio/FinLens/tests/fixtures/jpm-20241231.htm"

    jpm_html_content = None
    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            jpm_html_content = f.read()
        print(
            f"Successfully loaded HTML content from {html_file_path} (length: {len(jpm_html_content)} characters)."
        )
    except FileNotFoundError:
        print(
            f"ERROR: HTML file '{html_file_path}' not found. Please ensure it's in the correct location relative to the script or provide an absolute path."
        )
    except Exception as e:
        print(f"Error loading HTML file: {e}")

    if jpm_html_content:
        # These are the exact starting snippets from your analyzer output's largest chunks
        target_snippets = {
            "Glossary_Like_Text_Prefix":
            "2023 Form 10-K: Annual report on Form 10-K for the year ended December 31, 2023, filed with the U.S. Securities and Exchange Commission. ABS: Asset-backed securities Active foreclosures: Loans referred to foreclosure where formal foreclosure proceedings are ongoing. Includes both judicial and non-judicial states.AFS: Available-for-sale ALCO: Asset Liability CommitteeAlternative assets “Alternatives”: The following types of assets constitute alternative investments - hedge funds, currency, real e",
            "Operational_Risk_Text_Prefix":
            "Operational risk is the risk of an adverse outcome resulting from inadequate or failed internal processes or systems; human factors; or external events impacting the Firm’s processes or systems. Operational Risk includes compliance, conduct, legal, and estimations and model risk. Operational risk is inherent in the Firm’s activities and can manifest itself in various ways, including fraudulent acts, business disruptions (including those caused by extraordinary events beyond the Firm's control),"
        }

        # The shorter phrase you mentioned searching for
        target_snippets[
            "Your_Searched_Phrase"] = "2023 Form 10-K: Annual report on Form 10-K for the year ended December 31, 2023, filed"

        # A very common and simple phrase that should definitely be in a JPM 10-K
        target_snippets["Common_JPM_Phrase"] = "JPMorgan Chase & Co."

        search_results = count_raw_snippet_occurrences_revised(
            jpm_html_content, target_snippets)

        print("\n--- Revised Raw HTML Search Results for jpm-20241231.htm ---")
        for snippet_name, result in search_results.items():
            print(f"\nSnippet Category: {snippet_name}")
            print(
                f"  Searched for normalized prefix (first 100 chars): \"{result['searched_prefix_normalized']}\""
            )
            print(
                f"  Found {result['count_in_normalized_html']} time(s) in the normalized HTML."
            )
            print("-" * 40)
    else:
        print("Could not perform search as HTML content was not loaded.")
