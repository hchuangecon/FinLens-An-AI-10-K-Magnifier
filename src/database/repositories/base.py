# src/database/repositories/base.py

import logging
from abc import ABC  # Abstract Base Class
from sqlalchemy.orm import scoped_session, Session
from typing import Callable  # To type hint the session_factory

logger = logging.getLogger(__name__)

# Type alias for the session factory callable
SessionFactory = Callable[[], Session]


class AbstractRepository(ABC):
    """
    Abstract Base Class for all data repository classes.

    Ensures that all repositories are initialized with a session factory
    for consistent database access and session management.
    """

    def __init__(self, session_factory: SessionFactory):
        """
        Initializes the repository with a SQLAlchemy session factory.

        Args:
            session_factory: A callable (typically a scoped_session instance)
                             that returns a new Session object when called.
        """
        if not callable(session_factory):
            # Added check for callability
            raise TypeError("session_factory must be a callable object.")
        self.session_factory = session_factory
        logger.info(f"{self.__class__.__name__} initialized.")

    # Concrete repository subclasses will implement specific data access methods
    # (e.g., add_company, get_filing_by_accession, bulk_merge_companies)
    # using the self.session_factory, often with the get_session context manager.

    # Example of how the context manager might be used within a subclass method:
    #
    # from src.database.session import get_session # Import the context manager
    #
    # class CompanyRepository(AbstractRepository):
    #     def get_by_cik(self, cik: str) -> Optional[Company]:
    #         with get_session(self.session_factory) as session: # Use context manager
    #             try:
    #                 # Use SQLAlchemy 2.0 style select
    #                 stmt = select(Company).where(Company.cik == cik)
    #                 result = session.execute(stmt)
    #                 return result.scalar_one_or_none()
    #             except SQLAlchemyError as e:
    #                 logger.error(f"Error fetching company by CIK {cik}: {e}")
    #                 # Let the context manager handle rollback via re-raising
    #                 raise DatabaseQueryError(f"Failed to get company CIK {cik}")
