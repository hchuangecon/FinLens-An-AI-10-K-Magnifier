# src/database/repositories/company.py

import logging
from typing import List, Dict, Optional, Set, Sequence
from sqlalchemy import select, inspect, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert as mysql_insert

from .base import AbstractRepository, SessionFactory
from src.database.models import Company
from src.database.session import get_session
from src.core.exceptions import DatabaseError, DatabaseQueryError

logger = logging.getLogger(__name__)


class CompanyRepository(AbstractRepository):
    """
    Provides data access methods for Company entities.
    Uses MySQL INSERT ... ON DUPLICATE KEY UPDATE for bulk merging/upserting.
    """

    def __init__(self, session_factory: SessionFactory):
        """
        Initializes the repository with a SQLAlchemy session factory.

        Args:
            session_factory: A callable (typically a scoped_session instance)
                             that returns a new Session object when called.
        """
        super().__init__(session_factory)  # Initialize the base class

    def get_by_cik(self, cik: str) -> Optional[Company]:
        """Retrieves a single Company by its CIK."""
        logger.debug(f"Querying Company by CIK: {cik}")
        with get_session(self.session_factory) as session:
            try:
                stmt = select(Company).where(Company.cik == cik)
                result = session.execute(stmt)
                company = result.scalar_one_or_none()
                logger.debug(
                    f"Company query result for CIK {cik}: {'Found' if company else 'Not Found'}"
                )
                return company
            except SQLAlchemyError as e:
                logger.error(
                    f"Database error querying company by CIK {cik}: {e}",
                    exc_info=True)
                raise DatabaseQueryError(f"Failed to query company CIK {cik}")

    def get_existing_ciks(self, ciks_to_check: Sequence[str]) -> Set[str]:
        """Checks a list of CIKs against the database and returns the set of those that exist."""
        if not ciks_to_check:
            return set()
        logger.debug(f"Checking existence for {len(ciks_to_check)} CIKs.")
        existing_ciks: Set[str] = set()
        # Consider making batch_size configurable if needed
        batch_size = 10000
        ciks_list = list(ciks_to_check)
        with get_session(self.session_factory) as session:
            try:
                for i in range(0, len(ciks_list), batch_size):
                    batch = ciks_list[i:i + batch_size]
                    if not batch: continue
                    stmt = select(Company.cik).where(Company.cik.in_(batch))
                    results = session.execute(stmt)
                    existing_ciks.update(row.cik for row in results)
                logger.debug(
                    f"Found {len(existing_ciks)} existing CIKs out of {len(ciks_list)} checked."
                )
                return existing_ciks
            except SQLAlchemyError as e:
                logger.error(f"Database error checking CIK existence: {e}",
                             exc_info=True)
                raise DatabaseQueryError("Failed to check for existing CIKs")

    def bulk_upsert(self, company_mappings: List[Dict]) -> int:
        """
        Efficiently inserts new companies OR updates existing ones based on CIK
        using MySQL's INSERT ... ON DUPLICATE KEY UPDATE.

        Args:
            company_mappings: A list of dictionaries, where each dictionary represents
                              a company's data (must include 'cik' key and other fields
                              matching the Company model).

        Returns:
             The number of rows affected (as reported by MySQL, which counts
             inserts as 1 and updates as 2, unless unchanged then 0).

        Raises:
            DatabaseError: If the bulk operation fails.
        """
        if not company_mappings:
            logger.info("bulk_upsert called with empty list, no action taken.")
            return 0

        logger.info(
            f"Starting bulk upsert for {len(company_mappings)} company mappings using ON DUPLICATE KEY UPDATE."
        )

        # Filter out mappings that don't have a valid 'cik' key
        valid_mappings = [
            m for m in company_mappings
            if m.get('cik')  # Use .get() for safer access
        ]
        if len(valid_mappings) != len(company_mappings):
            original_count = len(company_mappings)
            valid_count = len(valid_mappings)
            logger.warning(
                f"Filtered out {original_count - valid_count} mappings missing 'cik' key or having empty CIK during bulk upsert."
            )
        if not valid_mappings:
            logger.warning(
                "No valid mappings remaining after filtering for CIK.")
            return 0

        company_table = Company.__table__
        mapper = inspect(Company)
        # Determine all unique keys present across all valid input dictionaries, excluding 'cik'
        all_keys = set(key for mapping in valid_mappings for key in mapping
                       if key != 'cik')

        # Prepare the dictionary for the ON DUPLICATE KEY UPDATE clause
        update_columns = {}
        for col in mapper.columns:
            # Only include columns that are:
            # 1. Not the primary key ('cik')
            # 2. Actually present in the input data (in 'all_keys')
            if not col.primary_key and col.name in all_keys:
                # Get the actual column object from the table definition
                column_object = company_table.c[col.name]
                # Use func.values() to explicitly generate VALUES(column_name) syntax
                update_columns[col.name] = func.values(column_object)

        if not update_columns:
            # This case could happen if the input mappings ONLY contain 'cik' after filtering
            logger.warning(
                "No columns (other than CIK) found in the input data to use for the "
                "ON DUPLICATE KEY UPDATE clause. Check input data structure. "
                "Attempting an INSERT IGNORE operation instead.")
            # Fallback to INSERT IGNORE if no update columns identified
            stmt = mysql_insert(company_table).values(valid_mappings)
            stmt = stmt.prefix_with("IGNORE", dialect="mysql")
        else:
            # Construct the standard INSERT ... ON DUPLICATE KEY UPDATE statement
            stmt = mysql_insert(company_table).values(valid_mappings)
            stmt = stmt.on_duplicate_key_update(**update_columns)

        affected_rows = 0
        with get_session(self.session_factory) as session:
            try:
                logger.info(
                    f"Executing bulk upsert statement for {len(valid_mappings)} companies..."
                )
                result = session.execute(stmt)
                # rowcount for ON DUPLICATE KEY UPDATE:
                # 1 for each new row inserted
                # 2 for each existing row updated (if value changed)
                # 0 for each existing row not updated (if value didn't change)
                affected_rows = result.rowcount
                logger.info(
                    f"Bulk upsert statement executed. MySQL 'rows affected': {affected_rows}"
                )
                # Commit is handled by the get_session context manager

            except SQLAlchemyError as e:
                logger.error(
                    f"Database error during bulk upsert operation: {e}",
                    exc_info=True)
                # Rollback is handled by get_session
                raise DatabaseError(f"Bulk upsert failed: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during bulk upsert: {e}",
                             exc_info=True)
                # Rollback is handled by get_session
                raise DatabaseError(
                    f"Unexpected error during bulk upsert: {e}")

        logger.info(
            f"Bulk upsert finished. MySQL affected rows: {affected_rows}.")
        return affected_rows
