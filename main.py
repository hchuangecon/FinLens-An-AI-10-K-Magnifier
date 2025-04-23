# main.py
import argparse
import logging
import os
import sys
from datetime import datetime, date, timedelta
from typing import Optional, Set

# --- Setup Logging ---
# Import and call setup_logging() BEFORE importing other project modules
# to ensure logging is configured early.
try:
    from src.config.logging_config import setup_logging
    setup_logging()
except ImportError as e:
    # Basic fallback logging if config fails
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.critical(
        f"Failed to import logging configuration: {e}. Using basic logging.")
    # Depending on severity, you might want to exit here.

# Now import other components
try:
    from src.services.pipeline import PipelineService
    from src.core.exceptions import FinlensError, DatabaseQueryError
except ImportError as e:
    logging.critical(
        f"Failed to import core pipeline components: {e}. Ensure src directory is in PYTHONPATH or use 'python -m main'.",
        exc_info=True)
    sys.exit(1)

# Get logger for this script
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="FinLens SEC Data Pipeline Orchestrator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # --- Mode Selection ---
    parser.add_argument(
        "--mode",
        choices=['bulk', 'incremental', 'backfill', 'download_docs'],
        required=True,
        help=
        ("Pipeline execution mode: "
         "'bulk' (download/extract/ingest submissions.zip; for first time setup), "
         "'incremental' (process daily indices for updates), "
         "'backfill' (process quarterly indices for history), "
         "'download_docs' (download specific filing documents)."))

    # --- Options for 'bulk' mode ---
    parser.add_argument("--skip-download",
                        action='store_true',
                        help="[Bulk Mode] Skip downloading submissions.zip.")
    parser.add_argument("--skip-extract",
                        action='store_true',
                        help="[Bulk Mode] Skip extracting submissions.zip.")
    parser.add_argument(
        "--skip-ingest",
        action='store_true',
        help="[Bulk Mode] Skip ingesting data from JSON files.")

    # --- Options for 'incremental' mode ---
    parser.add_argument(
        "--days-back",
        type=int,
        default=None,
        metavar='N',
        help=
        "[Incremental Mode] Number of past days to check for updates (default from settings)."
    )

    # --- Options for 'backfill' mode ---
    parser.add_argument(
        "--start-year",
        type=int,
        metavar='YYYY',
        help="[Backfill Mode] Required: Start year for historical backfill.")
    parser.add_argument(
        "--end-year",
        type=int,
        metavar='YYYY',
        help="[Backfill Mode] Required: End year for historical backfill.")
    parser.add_argument(
        "--backfill-forms",
        type=str,
        default=None,
        metavar='FORM1,FORM2',
        help=
        "[Backfill Mode] Comma-separated forms to include (e.g., '10-K,10-K/A'). Default from settings (processes all if None)."
    )

    # --- Options for 'download_docs' mode ---
    parser.add_argument(
        "--download-forms",
        type=str,
        default='10-K,10-K/A',
        metavar='FORM1,FORM2',
        help=
        "[Download Mode] Comma-separated target forms to download (e.g., '10-K,10-K/A')."
    )
    parser.add_argument(
        "--download-start-date",
        type=str,
        default=None,
        metavar='YYYY-MM-DD',
        help="[Download Mode] Start date for filings to download documents for."
    )
    parser.add_argument(
        "--download-end-date",
        type=str,
        default=None,
        metavar='YYYY-MM-DD',
        help="[Download Mode] End date for filings to download documents for.")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar='N',
        help=
        "[Download Mode] Max number of documents to query/prepare for download (most recent first if dates used)."
    )
    parser.add_argument(
        "--max-downloads",
        type=int,
        default=None,
        metavar='N',
        help=
        "[Download Mode] Max number of documents to actually download in this run."
    )
    parser.add_argument(
        "--download-threads",
        type=int,
        default=None,
        metavar='T',
        help=
        "[Download Mode] Number of parallel download threads (default from settings)."
    )
    parser.add_argument(
        "--no-skip-existing",
        action='store_true',
        help=
        "[Download Mode] Force download even if local file exists (default skips existing)."
    )

    return parser.parse_args()


def _parse_date(date_str: Optional[str]) -> Optional[date]:
    """Helper to parse YYYY-MM-DD date strings."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.error(
            f"Invalid date format: '{date_str}'. Please use YYYY-MM-DD.")
        return None  # Or raise error? Return None for now.


def _parse_forms(forms_str: Optional[str]) -> Optional[Set[str]]:
    """Helper to parse comma-separated form strings into a set."""
    if not forms_str:
        return None
    forms = {
        form.strip().upper()
        for form in forms_str.split(',') if form.strip()
    }
    return forms if forms else None


def main():
    """Main execution function."""
    args = parse_arguments()
    logger.info(f"Executing FinLens Pipeline in mode: {args.mode}")

    pipeline = None
    exit_code = 0  # Assume success unless error occurs

    try:
        # Initialize the pipeline service
        # Initialization errors are critical and will raise RuntimeError
        pipeline = PipelineService()

        # Execute based on mode
        if args.mode == 'bulk':
            logger.info("Running Bulk Process...")
            success = pipeline.run_bulk_process(
                download=(not args.skip_download),
                extract=(not args.skip_extract),
                ingest=(not args.skip_ingest))
            if not success: exit_code = 1

        elif args.mode == 'incremental':
            logger.info("Running Incremental Update...")
            success = pipeline.run_incremental_update(
                days_to_check=args.
                days_back  # Pass None if user didn't specify
            )
            if not success: exit_code = 1

        elif args.mode == 'backfill':
            logger.info("Running Historical Backfill...")
            if args.start_year is None or args.end_year is None:
                logger.error(
                    "Both --start-year and --end-year are required for backfill mode."
                )
                exit_code = 1
            else:
                forms_set = _parse_forms(args.backfill_forms)
                success = pipeline.run_historical_backfill(
                    start_year=args.start_year,
                    end_year=args.end_year,
                    forms_to_include=forms_set)
                if not success: exit_code = 1

        elif args.mode == 'download_docs':
            logger.info("Running Document Download Process...")
            # 1. Parse arguments needed for querying
            target_forms_set = _parse_forms(args.download_forms)
            start_date = _parse_date(args.download_start_date)
            end_date = _parse_date(args.download_end_date)

            if not target_forms_set:
                logger.error("No valid target forms specified for download.")
                exit_code = 1
            else:
                # 2. Query repository to get filings to process
                logger.info(
                    f"Querying database for filings matching criteria...")
                try:
                    # Use the dedicated repository method which includes SIC filtering
                    filings_to_process = pipeline.filing_repo.find_filings_for_download(
                        form_types=list(
                            target_forms_set),  # Pass as list or sequence
                        start_date=start_date,
                        end_date=end_date,
                        limit=args.limit  # Pass limit to DB query
                    )

                    # 3. Call the download service method
                    if filings_to_process:
                        success_count, failure_count = pipeline.download_filing_documents(
                            filings_to_process=filings_to_process,
                            target_forms=
                            target_forms_set,  # Pass target forms for HTML parser
                            num_threads=args.
                            download_threads,  # Pass None if user didn't specify
                            max_downloads=args.
                            max_downloads,  # Pass None if user didn't specify
                            skip_existing=(not args.no_skip_existing))
                        # Decide if partial success is an error? For now, just log counts.
                        if failure_count > 0:
                            exit_code = 1  # Consider run failed if any download fails

                    else:
                        logger.info(
                            "No filings found matching the specified criteria for download."
                        )

                except DatabaseQueryError as e:
                    logger.error(
                        f"Database query failed while finding filings to download: {e}"
                    )
                    exit_code = 1
                except Exception as e:  # Catch other unexpected errors during query/prep
                    logger.error(f"Error preparing for document download: {e}",
                                 exc_info=True)
                    exit_code = 1

        else:
            # Should not happen if argparse choices are set correctly
            logger.error(f"Unknown mode: {args.mode}")
            exit_code = 1

    except FinlensError as fe:
        logger.critical(f"A pipeline error occurred: {fe}", exc_info=True)
        exit_code = 1
    except RuntimeError as rte:  # Catch init errors from PipelineService
        logger.critical(f"Pipeline service failed to initialize: {rte}",
                        exc_info=True)
        exit_code = 1
    except Exception as e:
        logger.critical(
            f"An unexpected error occurred during pipeline execution: {e}",
            exc_info=True)
        exit_code = 1
    finally:
        if pipeline:
            logger.info("Closing pipeline resources...")
            pipeline.close()  # Ensure DB engine is disposed
        logger.info(f"Pipeline finished with exit code {exit_code}.")
        sys.exit(exit_code)  # Exit with code 0 on success, 1 on error


if __name__ == "__main__":
    main()
