# src/database/session.py
import logging
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError

from . import models
from src.config.settings import DatabaseSettings  # Import specific settings class
from src.core.exceptions import DatabaseConnectionError, DatabaseError  # Import custom exceptions

logger = logging.getLogger(__name__)  # Use specific logger


def create_database_if_not_exists(db_settings: DatabaseSettings):
    """Creates the database specified in settings if it doesn't exist."""
    # Connect without specifying the database name first
    server_url = f"mysql+mysqlconnector://{db_settings.user}:{db_settings.password}@{db_settings.host}:{db_settings.port}/?charset=utf8mb4"
    temp_engine = None
    try:
        logger.info(
            f"Checking/creating database '{db_settings.name}' on {db_settings.host}..."
        )
        temp_engine = create_engine(server_url, echo=False)
        with temp_engine.connect() as connection:
            # Use text() for literals, especially CREATE DATABASE
            connection.execute(
                text(
                    f"CREATE DATABASE IF NOT EXISTS `{db_settings.name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                ))
            # connection.commit() # CREATE DATABASE often auto-commits or doesn't need explicit commit in this context
        logger.info(f"Database '{db_settings.name}' checked/created.")
        return True
    except SQLAlchemyError as e:
        logger.error(
            f"Failed to check/create database '{db_settings.name}': {e}",
            exc_info=True)
        # Raise our custom exception
        raise DatabaseConnectionError(
            f"Failed to check/create database '{db_settings.name}': {e}")
    except Exception as e:  # Catch unexpected errors
        logger.error(f"Unexpected error during database check/creation: {e}",
                     exc_info=True)
        raise DatabaseConnectionError(
            f"Unexpected error during database check/creation: {e}")
    finally:
        if temp_engine:
            temp_engine.dispose()


def create_database_tables(engine):
    """Creates the database tables based on SQLAlchemy models."""
    logger.info("Creating database tables if they don't exist...")
    try:
        # Assumes Base is imported from models module
        models.Base.metadata.create_all(bind=engine)
        logger.info("Database tables checked/created successfully.")
        return True
    except SQLAlchemyError as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        # Raise a general database error
        raise DatabaseError(f"Failed to create database tables: {e}")
    except Exception as e:
        logger.error(f"Unexpected error creating database tables: {e}",
                     exc_info=True)
        raise DatabaseError(f"Unexpected error creating database tables: {e}")


def initialize_database(db_settings: DatabaseSettings):
    """
    Creates DB if needed, creates engine and session factory, creates tables.

    Args:
        db_settings: An instance of DatabaseSettings containing connection details.

    Returns:
        A tuple: (engine, session_factory) on success.

    Raises:
        DatabaseConnectionError: If connection or initial setup fails.
        DatabaseError: If table creation fails.
    """
    # Ensure database exists first
    create_database_if_not_exists(db_settings)

    # Now connect to the specific database
    DATABASE_URL = (
        f"mysql+mysqlconnector://{db_settings.user}:{db_settings.password}@"
        f"{db_settings.host}:{db_settings.port}/{db_settings.name}?charset=utf8mb4"
    )
    engine = None
    try:
        logger.info(
            f"Connecting to database '{db_settings.name}' and creating engine..."
        )
        engine = create_engine(
            DATABASE_URL,
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_size=10,  # Default pool size
            max_overflow=20,  # Allow 20 extra connections under load
            pool_pre_ping=True,  # Check connection validity before use
            echo=False)

        # Create the session factory (scoped for thread safety)
        session_factory = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=engine))
        logger.info("Database engine and session factory created.")

        # Create tables using the newly created engine
        # This raises DatabaseError on failure
        create_database_tables(engine)

        # Return the engine and session factory on success
        logger.info("Database initialization successful.")
        return engine, session_factory

    except SQLAlchemyError as e:
        # Catch connection errors specifically during engine creation or session setup
        logger.error(
            f"Failed to connect or setup session/tables for '{db_settings.name}': {e}",
            exc_info=True)
        if engine: engine.dispose()  # Clean up engine if created
        raise DatabaseConnectionError(
            f"Failed to establish database connection or session: {e}")
    except DatabaseError:
        # Re-raise DatabaseError from create_database_tables
        if engine: engine.dispose()
        raise
    except Exception as e:
        # Catch any other unexpected error
        logger.error(
            f"Unexpected error during database initialization steps: {e}",
            exc_info=True)
        if engine: engine.dispose()
        raise DatabaseError(
            f"Unexpected error during database initialization: {e}")


# Optional: Context manager for sessions (useful within repositories)
from contextlib import contextmanager


@contextmanager
def get_session(session_factory):
    """Provide a transactional scope around a series of operations."""
    session = session_factory()
    logger.debug(f"DB Session {id(session)} acquired.")
    try:
        yield session
        logger.debug(f"DB Session {id(session)} committing.")
        session.commit()
    except Exception as e:
        logger.warning(
            f"DB Session {id(session)} rolling back due to: {e.__class__.__name__}"
        )
        session.rollback()
        raise  # Re-raise the exception after rollback
    finally:
        logger.debug(f"DB Session {id(session)} closing.")
        # session_factory.remove() # Crucial for scoped_session to clean up thread-local session
        # Let's rely on the caller (or framework if using one like FastAPI) to remove the session
        # Or make the context manager responsible for it:
        session_factory.remove()
