## Filtering Non-Operating Companies (ABS/Funds) using SIC Codes

To focus the 10-K analysis primarily on standard operating companies, this pipeline implements filtering based on Standard Industrial Classification (SIC) codes during the 10-K query phase (`query_filter_and_download_10k_filings`).

**Why Filter?**

Certain entity types, particularly Asset-Backed Securities (ABS) issuers and various investment funds, file 10-K forms that follow specialized reporting requirements (e.g., SEC General Instruction J) and typically do not contain the standard four financial statements or Management's Discussion & Analysis (MD&A) relevant for analyzing operating businesses. Including these can skew results or require separate handling.

**Filtered SIC Codes:**

The following SIC codes are currently excluded by default:

*   **`6189`**: Asset-Backed Securities (Most common code for auto loan trusts, mortgage-backed securities, etc.)
*   **`6722`**: Management Investment Offices, Open-End (Primarily Mutual Funds)
*   **`6726`**: Investment Offices, Not Elsewhere Classified (Includes Closed-End Funds, Unit Investment Trusts, etc.)

**Implementation:**

*   The `query_filter_and_download_10k_filings` function modifies its database query.
*   It joins the `filings` table with the `companies` table.
*   It adds a `WHERE` clause: `companies.sic NOT IN ('6189', '6722', '6726')`. This excludes filings where the company's SIC code matches one of the filtered codes.
*   Filings from companies where the SIC code is `NULL` (not available in the database) **are currently included** in the results, as they might still be operating companies.

**Prerequisite:**

*   This filtering relies on the `companies` table in your database having an `sic` column (e.g., `VARCHAR(4)`).
*   This `sic` column must be populated with the correct 4-digit SIC code during data ingestion (either from the bulk submissions JSON files or via API lookup when new CIKs are added). Ensure the `parse_cik_json` helper function and the new company logic in `incremental_update` correctly extract and store the SIC code.

**Limitations:**

*   The accuracy depends entirely on the SIC code data stored in your `companies` table being correct and comprehensive.
*   Some niche entities might use different SIC codes or be misclassified. This filter catches the vast majority of common ABS/fund structures but isn't guaranteed to be 100% exhaustive.