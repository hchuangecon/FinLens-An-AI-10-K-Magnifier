# FinLens SEC EDGAR Data Pipeline Documentation

## Overview

This pipeline automates the process of fetching SEC filing metadata, storing it in a MySQL database, and optionally downloading specific filing documents (like 10-K annual reports). It uses official SEC data sources (Bulk Submissions Zip, Daily Index Files) and is designed for efficiency using parallel processing and robust data handling. The goal is to build a local database suitable for financial analysis and research.

## Prerequisites

1. **Python:** Python 3.10 or higher recommended.
2. **pip:** Python package installer.
3. **Dependencies:** Install required Python packages:
   ```bash
   pip install requests beautifulsoup4 lxml sqlalchemy mysql-connector-python python-dotenv schedule argparse
   ```
   *(Consider creating a `requirements.txt` file)*
4. **MySQL Database:** A running MySQL server instance (version 5.7+ or 8.0+ recommended).
5. **`.env` File:** Create a file named `.env` in the project root directory (`FinLens/`) and add the following environment variables:
   ```dotenv
   # Database Credentials
   DB_HOST=your_db_host # (e.g., localhost)
   DB_PORT=3306         # (or your db port)
   DB_USER=your_db_username
   DB_PASSWORD=your_db_password
   DB_NAME=sec_filings  # (or your preferred database name)

   # SEC EDGAR User-Agent (REQUIRED - Replace with your info)
   SEC_USER_AGENT=YourCompanyName YourName contact@yourdomain.com
   ```
   * **CRITICAL:** Replace the placeholder `SEC_USER_AGENT` with your actual information per SEC guidelines (Format: `CompanyName ContactName ContactEmail`). Failure to identify yourself can lead to being blocked.

## Database Schema

The script interacts with two main tables (defined in `finlens/database/models.py`):

1. **`companies` Table:** Stores information about each filing entity (identified by CIK).
   * `cik` (Primary Key): Central Index Key (10 digits).
   * `name`: Company/Entity Name.
   * `sic`: 4-digit Standard Industrial Classification code (used for filtering).
   * `entity_type`: Type of entity (e.g., 'operating', 'investment').
   * `sic_description`: Text description of the SIC code.
   * `insider_trade_owner`: Boolean indicating if owner insider transactions exist.
   * `insider_trade_issuer`: Boolean indicating if issuer insider transactions exist.
   * `business_street1`, `business_street2`, `business_city`: Components of the business address.
   * `business_state_or_country`: US State code or Foreign Country indicator from the business address. **Used for location queries.**
   * `business_state_or_country_desc`: Description matching `business_state_or_country`.
   * `business_zip`: Business address zip code.
   * `phone`: Company phone number.

2. **`filings` Table:** Stores metadata for each individual filing.
   * `id` (Primary Key): Auto-incrementing ID.
   * `cik` (Foreign Key to `companies.cik`): Links the filing to the company.
   * `form_type`: The type of form filed (e.g., '10-K', '8-K', '4').
   * `filing_date`: The date the filing was accepted by the SEC.
   * `accession_number` (Unique Key): The unique identifier for the filing submission.
   * `primary_document_filename`: The filename of the primary document as listed in the SEC source data (might be `.txt` or `.htm`).

**Schema Updates:** The script will attempt to create these tables if they don't exist. If you modify the models (e.g., add columns), you **must** update your database schema manually (using `ALTER TABLE`) or by dropping/recreating tables before running the bulk ingest again.

## Setup

1. Clone/obtain project files.
2. Navigate (`cd`) to the project root directory (e.g., `FinLens/`).
3. Create/activate a Python virtual environment (recommended).
4. Install dependencies (`pip install -r requirements.txt` or manually).
5. Create and populate the `.env` file in the project root.
6. Ensure MySQL server is running and the user specified in `.env` has necessary privileges (CREATE DATABASE, CREATE TABLE, INSERT, SELECT, UPDATE, DELETE).

## Execution

Run the script from the **project root directory** using the `python -m` flag to ensure correct module imports (or run directly if the `sys.path` modification in `__main__` works for your setup).

```bash
python -m finlens.extraction.sec_edgar_pipeline --mode <MODE> [OPTIONS]
```

### Execution Modes (--mode)

* **bulk**:
  * Action: Downloads the full submissions.zip, extracts it (overwriting existing data/submissions/), parses all local JSONs in parallel, merges company data, and bulk-inserts unique filings using INSERT IGNORE.
  * Use Case: Required for first-time database population. Can be used for infrequent full data refreshes.
  * Notes: Very time-consuming, high bandwidth/disk usage. Assumes companies table includes the sic column.

* **bulk_ingest_only**:
  * Action: Skips download and extraction. Parses existing local JSON files in data/submissions/, merges company data, and bulk-inserts unique filings using INSERT IGNORE.
  * Use Case: Re-populating the database from previously extracted files (e.g., after clearing tables or changing DB schema) without re-downloading the large zip. Requires data/submissions/ to contain the extracted files.

* **incremental**:
  * Action: Downloads recent SEC Daily Index files (master.idx). Finds filings from the last ~5 days not already in the local database. Inserts these new filings. Uses the SEC API to fetch full details (including SIC, address) for any CIKs associated with new filings and adds/updates them in the companies table.
  * Use Case: Recommended mode for regular (e.g., daily) updates after an initial bulk load. Efficiently keeps the database current.

### Command-Line Arguments ([OPTIONS])

* `--mode {bulk,bulk_ingest_only,incremental}`: (Required) Selects the operation mode.
* `--data-path PATH`: Root directory for data storage (default: data).
* `--start-year YEAR`: Start year for 10-K query (default: 2020).
* `--end-year YEAR`: End year (inclusive) for 10-K query (default: current year).
* `--download-10k`: Flag to enable 10-K document downloading phase (default: disabled).
* `--max-10k-downloads N`: Limit number of 10-Ks downloaded (most recent first) (default: no limit).
* `--bulk-workers N`: Number of processes for parallel bulk parsing (default: CPU cores - 1).
* `--download-threads N`: Number of threads for parallel downloading (default: 10).
* `--log-level {DEBUG,INFO,WARNING,ERROR}`: Console/file logging verbosity (default: INFO).

### Example Usage

```bash
# 1. Initial Bulk Load (Populates DB with SIC codes)
# Ensure database schema is updated first!
python -m finlens.extraction.sec_edgar_pipeline --mode bulk --bulk-workers 7

# 2. Daily Incremental Update
python -m finlens.extraction.sec_edgar_pipeline --mode incremental

# 3. Incremental Update + Download Filtered 10-Ks for 2022 (Max 100)
python -m finlens.extraction.sec_edgar_pipeline --mode incremental --start-year 2022 --end-year 2022 --download-10k --max-10k-downloads 100

# 4. Re-ingest from existing local files after clearing DB
# (Ensure data/submissions contains CIK*.json files)
# >> Manually run: TRUNCATE TABLE filings; TRUNCATE TABLE companies; <<
python -m finlens.extraction.sec_edgar_pipeline --mode bulk_ingest_only --bulk-workers 7
```

## Core Logic Details

* **Bulk Ingestion**: Uses multiprocessing for parsing local JSONs. Extracts company details including SIC, address, etc. Uses bulk_merge_mappings for companies (handles inserts/updates based on CIK primary key) and batched INSERT IGNORE for filings (efficiently skips inserting filings with existing accession numbers).

* **Incremental Update**: Uses SEC Daily Index files (master.idx) for efficiency. Identifies new filings not present in the local DB by comparing accession numbers. Inserts only new filings using INSERT IGNORE. For CIKs associated with new filings, uses the SEC Submissions API to fetch full, current company details (including SIC, address etc.) and uses bulk_merge_mappings to add these companies if they don't exist or update their details if they do.

* **10-K Query/Download**:
  * Queries local DB for 10-Ks within the specified date range.
  * Filters out non-operating entities using WHERE companies.sic NOT IN ('6189', '6722', '6726'). Also includes companies where sic is NULL to avoid potentially filtering out relevant operating companies whose SIC wasn't captured.
  * For each remaining 10-K, fetches the SEC filing index page (...-index.html) to find the primary HTM document filename using BeautifulSoup.
  * Checks the index page table for common ABS exhibit types (EX-33, EX-34, etc.) as a secondary filter to flag potential ABS filings missed by the SIC code filter.
  * If an HTM file is found and the filing is not flagged as ABS (by the index page check), it prioritizes downloading the HTM file.
  * If no HTM is found (and not flagged as ABS), it falls back to downloading the filename stored in the database (which might be .txt).
  * Uses ThreadPoolExecutor for concurrent, rate-limited downloads.
  * Saves metadata about the filtered (post-SIC check) 10-Ks found by the database query to a JSON file.

## Data Storage

* `data/submissions/`: Stores extracted CIK*.json files from bulk download. Can be very large. Used by bulk_ingest_only.
* `data/10k_documents/`: Stores downloaded 10-K documents (if --download-10k is enabled). Can become very large.
* `data/10k_filings_metadata_YYYY_YYYY_filtered.json`: JSON summary of 10-Ks found by the query after SIC filtering.

## Important Notes

* **SEC User-Agent**: Mandatory. Set the SEC_USER_AGENT environment variable correctly in .env.
* **Rate Limiting**: The script enforces a ~10 requests/second limit to SEC servers. Do not disable this.
* **SIC Code Accuracy**: The effectiveness of filtering depends on having accurate SIC codes in your companies table. Ensure your bulk ingestion (parse_cik_json) correctly captures this and that the incremental update fetches it for new companies. Missing or incorrect SICs can lead to imperfect filtering.
* **Disk Space**: Bulk mode and document downloading require significant disk space (potentially hundreds of GB or more).
* **Error Handling**: The script includes error handling and logging. Review sec_pipeline.log for details on warnings or errors. Database constraint errors during bulk load are often skipped (INSERT IGNORE), while errors during incremental API lookups or downloads might cause specific CIKs/files to be skipped.