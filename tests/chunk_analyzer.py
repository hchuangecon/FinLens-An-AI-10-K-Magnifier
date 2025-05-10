import json
import statistics
from typing import List, Dict, Any, Optional

# --- Token Estimation Rules of Thumb ---
CHARS_PER_TOKEN_ESTIMATE = 4.0
WORDS_PER_TOKEN_ESTIMATE = 0.75


def estimate_tokens_from_chars(char_count: int) -> float:
    return char_count / CHARS_PER_TOKEN_ESTIMATE


def estimate_tokens_from_words(word_count: int) -> float:
    return word_count / WORDS_PER_TOKEN_ESTIMATE


def analyze_and_identify_large_chunks(
        file_path: str,
        top_n_largest: int = 3,
        content_snippet_length: int = 500) -> Optional[Dict[str, Any]]:
    """
    Analyzes 'text_content' of TEXT and SUPPLEMENTARY nodes, identifies the largest ones,
    and provides overall statistics.

    Args:
        file_path: Path to the JSON file containing the list of FinLensNode objects.
        top_n_largest: Number of largest chunks to detail.
        content_snippet_length: Length of the text_content snippet to print for large chunks.

    Returns:
        A dictionary with analysis results, or None if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            nodes: List[Dict[str, Any]] = json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {file_path}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading the file: {e}")
        return None

    # Create a lookup for parent titles
    node_map: Dict[str, Dict[str, Any]] = {
        node['node_id']: node
        for node in nodes
    }

    text_chunks: List[Dict[str, Any]] = [
    ]  # Store dicts with node info and content length

    relevant_node_types = {"TEXT", "SUPPLEMENTARY"}
    processed_node_count = 0

    for node in nodes:
        node_type = node.get("node_type")
        text_content = node.get("text_content")

        if node_type in relevant_node_types:
            processed_node_count += 1
            if isinstance(text_content, str) and text_content.strip():
                char_count = len(text_content)
                word_count = len(text_content.split())
                text_chunks.append({
                    "node_id":
                    node.get("node_id"),
                    "parent_node_id":
                    node.get("parent_node_id"),
                    "section_id":
                    node.get("section_id"),
                    "char_count":
                    char_count,
                    "word_count":
                    word_count,
                    "text_content_snippet":
                    text_content[:content_snippet_length]  # Store snippet
                })

    if not text_chunks:
        print(
            f"No relevant text content found in nodes of type {relevant_node_types} for analysis."
        )
        # Return structure consistent with previous version for overall stats
        return {
            "filter_tables_excluded":
            True,  # Implicitly true for this function's focus
            "total_relevant_type_nodes": processed_node_count,
            "analyzed_chunks_with_content": 0,
            "avg_chars": 0,
            "min_chars": 0,
            "max_chars": 0,
            "std_dev_chars": 0,
            "avg_words": 0,
            "min_words": 0,
            "max_words": 0,
            "std_dev_words": 0,
            "avg_tokens_from_chars": 0,
            "min_tokens_from_chars": 0,
            "max_tokens_from_chars": 0,
            "avg_tokens_from_words": 0,
            "min_tokens_from_words": 0,
            "max_tokens_from_words": 0,
            "largest_chunks_details": []
        }

    char_counts = [chunk["char_count"] for chunk in text_chunks]
    word_counts = [chunk["word_count"] for chunk in text_chunks]

    # Sort chunks by character count in descending order to find the largest
    sorted_chunks = sorted(text_chunks,
                           key=lambda x: x["char_count"],
                           reverse=True)

    largest_chunks_details = []
    for i in range(min(top_n_largest, len(sorted_chunks))):
        large_chunk = sorted_chunks[i]
        parent_title = "N/A (Root or Parent not found)"
        if large_chunk["parent_node_id"] and large_chunk[
                "parent_node_id"] in node_map:
            parent_node = node_map[large_chunk["parent_node_id"]]
            parent_title = parent_node.get("title",
                                           "N/A (Parent has no title)")

        largest_chunks_details.append({
            "rank":
            i + 1,
            "node_id":
            large_chunk["node_id"],
            "char_count":
            large_chunk["char_count"],
            "word_count":
            large_chunk["word_count"],
            "est_tokens_chars":
            estimate_tokens_from_chars(large_chunk["char_count"]),
            "est_tokens_words":
            estimate_tokens_from_words(large_chunk["word_count"]),
            "section_id":
            large_chunk["section_id"],
            "parent_node_id":
            large_chunk["parent_node_id"],
            "parent_title":
            parent_title,
            "text_content_snippet":
            large_chunk["text_content_snippet"] +
            "..." if len(large_chunk["text_content_snippet"])
            == content_snippet_length else large_chunk["text_content_snippet"]
        })

    results = {
        "filter_tables_excluded":
        True,  # By definition for this function
        "total_relevant_type_nodes":
        processed_node_count,
        "analyzed_chunks_with_content":
        len(char_counts),
        "avg_chars":
        statistics.mean(char_counts) if char_counts else 0,
        "min_chars":
        min(char_counts) if char_counts else 0,
        "max_chars":
        max(char_counts) if char_counts else 0,
        "std_dev_chars":
        statistics.stdev(char_counts) if len(char_counts) > 1 else 0,
        "avg_words":
        statistics.mean(word_counts) if word_counts else 0,
        "min_words":
        min(word_counts) if word_counts else 0,
        "max_words":
        max(word_counts) if word_counts else 0,
        "std_dev_words":
        statistics.stdev(word_counts) if len(word_counts) > 1 else 0,
        "largest_chunks_details":
        largest_chunks_details
    }

    # Add overall token estimates to results
    results["avg_tokens_from_chars"] = estimate_tokens_from_chars(
        results["avg_chars"])
    results["min_tokens_from_chars"] = estimate_tokens_from_chars(
        results["min_chars"])
    results["max_tokens_from_chars"] = estimate_tokens_from_chars(
        results["max_chars"])

    results["avg_tokens_from_words"] = estimate_tokens_from_words(
        results["avg_words"])
    results["min_tokens_from_words"] = estimate_tokens_from_words(
        results["min_words"])
    results["max_tokens_from_words"] = estimate_tokens_from_words(
        results["max_words"])

    return results


if __name__ == "__main__":
    # Ensure this path points to your jpm_nodes_output.json file
    json_file_path = "/Users/mileshuang/Desktop_Mac_Studio/FinLens/jpm_nodes_output.json"

    # Analyze chunks, focusing on TEXT and SUPPLEMENTARY, and detail the top 5 largest
    analysis_data = analyze_and_identify_large_chunks(
        json_file_path, top_n_largest=5, content_snippet_length=500)

    if analysis_data:
        print(
            f"\n--- Overall Chunk Analysis Results for {json_file_path} (TABLES EXCLUDED) ---"
        )
        print(
            f"Total Nodes of Type TEXT/SUPPLEMENTARY: {analysis_data['total_relevant_type_nodes']}"
        )
        print(
            f"Number of Chunks Analyzed (with non-empty text_content): {analysis_data['analyzed_chunks_with_content']}"
        )

        print("\nCharacter Counts (TEXT/SUPPLEMENTARY only):")
        print(f"  Average: {analysis_data['avg_chars']:.2f}")
        print(f"  Min:     {analysis_data['min_chars']}")
        print(f"  Max:     {analysis_data['max_chars']}")
        print(f"  Std Dev: {analysis_data['std_dev_chars']:.2f}")

        print("\nWord Counts (TEXT/SUPPLEMENTARY only):")
        print(f"  Average: {analysis_data['avg_words']:.2f}")
        print(f"  Min:     {analysis_data['min_words']}")
        print(f"  Max:     {analysis_data['max_words']}")
        print(f"  Std Dev: {analysis_data['std_dev_words']:.2f}")

        print(
            "\nEstimated Token Counts (based on Characters ~4 chars/token - TEXT/SUPPLEMENTARY only):"
        )
        print(f"  Average: {analysis_data['avg_tokens_from_chars']:.2f}")
        print(f"  Min:     {analysis_data['min_tokens_from_chars']:.2f}")
        print(f"  Max:     {analysis_data['max_tokens_from_chars']:.2f}")

        print(
            "\nEstimated Token Counts (based on Words ~0.75 words/token - TEXT/SUPPLEMENTARY only):"
        )
        print(f"  Average: {analysis_data['avg_tokens_from_words']:.2f}")
        print(f"  Min:     {analysis_data['min_tokens_from_words']:.2f}")
        print(f"  Max:     {analysis_data['max_tokens_from_words']:.2f}")

        print(
            f"\n\n--- Details of Top {len(analysis_data['largest_chunks_details'])} Largest TEXT/SUPPLEMENTARY Chunks ---"
        )
        for chunk_detail in analysis_data['largest_chunks_details']:
            print(f"\nRank: {chunk_detail['rank']}")
            print(f"  Node ID: {chunk_detail['node_id']}")
            print(f"  Parent Node ID: {chunk_detail['parent_node_id']}")
            print(f"  Parent Title: {chunk_detail['parent_title']}")
            print(f"  Section ID: {chunk_detail['section_id']}")
            print(f"  Char Count: {chunk_detail['char_count']}")
            print(f"  Word Count: {chunk_detail['word_count']}")
            print(
                f"  Est. Tokens (Chars): {chunk_detail['est_tokens_chars']:.2f}"
            )
            print(
                f"  Est. Tokens (Words): {chunk_detail['est_tokens_words']:.2f}"
            )
            print(
                f"  Content Snippet ({len(chunk_detail['text_content_snippet'])} chars):\n    \"{chunk_detail['text_content_snippet']}\""
            )
