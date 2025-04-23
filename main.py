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
    parser.add_argument(
        "--bulk-chunk-size",
        type=int,
        default=100000,  # Default is 100000, rely on settings if RAM is full
        metavar='N',
        help=(
            "[Bulk Mode] Number of JSON files to process per chunk during "
            "ingestion to manage memory. Overrides setting/env var if provided."
        ))

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
    exit_code = 0

    try:
        # Initialize the pipeline service AFTER parsing args
        # This loads settings initially from .env and defaults
        pipeline = PipelineService()

        # --- Apply Command-Line Overrides to Settings ---
        # Check if command-line args should override loaded settings
        # Important: Do this AFTER pipeline init but BEFORE calling run methods

        if args.mode == 'bulk' and args.bulk_chunk_size is not None:
            if args.bulk_chunk_size > 0:
                logger.info(
                    f"Overriding bulk chunk size from settings with command-line value: {args.bulk_chunk_size}"
                )
                pipeline.settings.pipeline.bulk_ingest_file_chunk_size = args.bulk_chunk_size
            else:
                logger.warning(
                    "Ignoring invalid command-line --bulk-chunk-size (must be > 0). Using value from settings."
                )

        if args.mode == 'download_docs' and args.download_threads is not None:
            if args.download_threads > 0:
                logger.info(
                    f"Overriding download threads from settings with command-line value: {args.download_threads}"
                )
                pipeline.settings.pipeline.download_threads = args.download_threads
            else:
                logger.warning(
                    "Ignoring invalid command-line --download-threads (must be > 0). Using value from settings."
                )
        # Add similar blocks here if you add command-line overrides for
        # other settings like --days-back, etc.
        # --- End Overrides ---

        # Execute based on mode
        if args.mode == 'bulk':
            logger.info("Running Bulk Process...")
            # The pipeline will now use the potentially overridden chunk size setting
            success = pipeline.run_bulk_process(
                download=(not args.skip_download),
                extract=(not args.skip_extract),
                ingest=(not args.skip_ingest))
            if not success: exit_code = 1

        elif args.mode == 'incremental':
            logger.info("Running Incremental Update...")
            # Pass the command-line arg value directly if provided,
            # otherwise the method uses the setting default
            success = pipeline.run_incremental_update(
                days_to_check=args.days_back)
            if not success: exit_code = 1

        elif args.mode == 'backfill':
            logger.info("Running Historical Backfill...")
            if args.start_year is None or args.end_year is None:
                logger.error(
                    "Both --start-year and --end-year are required for backfill mode."
                )
                exit_code = 1
            elif args.start_year > args.end_year:
                logger.error("--start-year cannot be after --end-year.")
                exit_code = 1
            else:
                # Pass the parsed forms directly, method uses setting if None
                forms_set = _parse_forms(args.backfill_forms)
                success = pipeline.run_historical_backfill(
                    start_year=args.start_year,
                    end_year=args.end_year,
                    forms_to_include=forms_set)
                if not success: exit_code = 1

        elif args.mode == 'download_docs':
            logger.info("Running Document Download Process...")
            # 1. Parse arguments needed for querying
            # Use pipeline setting as default if CLI arg is None
            target_forms_set = _parse_forms(args.download_forms)
            if target_forms_set is None:
                target_forms_set = pipeline.settings.pipeline.target_primary_doc_forms
                logger.info(
                    f"Using target forms from settings: {target_forms_set}")

            start_date = _parse_date(args.download_start_date)
            end_date = _parse_date(args.download_end_date)

            if not target_forms_set:  # Check after potentially getting from settings
                logger.error(
                    "No valid target forms specified or found in settings for download."
                )
                exit_code = 1
            else:
                # 2. Query repository to get filings to process
                logger.info(
                    f"Querying database for filings matching criteria...")
                try:
                    # Use the dedicated repository method which includes SIC filtering
                    filings_to_process = pipeline.filing_repo.find_filings_for_download(
                        form_types=list(target_forms_set),
                        start_date=start_date,
                        end_date=end_date,
                        limit=args.limit)

                    # 3. Call the download service method
                    if filings_to_process:
                        # Pass the CLI arg value directly to the method,
                        # the method will use the setting if None is passed
                        success_count, failure_count = pipeline.download_filing_documents(
                            filings_to_process=filings_to_process,
                            target_forms=
                            target_forms_set,  # Pass effective target forms
                            num_threads=args.
                            download_threads,  # Pass CLI value (or None)
                            max_downloads=args.max_downloads,
                            skip_existing=(not args.no_skip_existing))

                        if failure_count > 0:
                            # Consider run partially failed if any download fails
                            logger.warning(
                                f"Document download completed with {failure_count} failures."
                            )
                            # Keep exit_code = 0 unless a critical error occurred? Or set to 1? Let's set to 1 on failure.
                            exit_code = 1
                    else:
                        logger.info(
                            "No filings found matching the specified criteria for download."
                        )

                except DatabaseQueryError as e:
                    logger.error(
                        f"Database query failed while finding filings to download: {e}"
                    )
                    exit_code = 1
                except Exception as e:
                    logger.error(
                        f"Error during document download process: {e}",
                        exc_info=True)
                    exit_code = 1

        else:
            logger.error(f"Unknown mode: {args.mode}")
            exit_code = 1

    except FinlensError as fe:
        logger.critical(f"A pipeline error occurred: {fe}", exc_info=True)
        exit_code = 1
    except RuntimeError as rte:
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
            pipeline.close()
        logger.info(f"Pipeline finished with exit code {exit_code}.")
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
