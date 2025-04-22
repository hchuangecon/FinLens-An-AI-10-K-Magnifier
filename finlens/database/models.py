# models.py
# finlens/database/models.py
import os
import sys
from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, scoped_session
from sqlalchemy import create_engine, Column, Integer, String, Date, ForeignKey, Index, Boolean, text
from sqlalchemy.dialects.mysql import TEXT
import logging

# Load environment variables
load_dotenv()

# Basic Logging Setup
logger = logging.getLogger('sec_pipeline.db')
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()])

# Remove global engine and SessionLocal variables here
# engine = None
# SessionLocal = None

# Base class for declarative models
Base = declarative_base()


# Define Database Models (Company, Filing - unchanged from your version)
class Company(Base):
    __tablename__ = 'companies'
    cik = Column(String(10), primary_key=True, unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    sic = Column(String(4), nullable=True, index=True)

    # --- NEW REQUESTED FIELDS ---
    entity_type = Column(String(50), nullable=True,
                         index=True)  # e.g., 'operating'
    sic_description = Column(String(255), nullable=True)
    insider_trade_owner = Column(Boolean, nullable=True)
    insider_trade_ssuer = Column(Boolean, nullable=True)
    # Address Info (Focus on Business Address for querying state/offshore)
    business_street1 = Column(String(255), nullable=True)
    business_street2 = Column(String(255), nullable=True)
    business_city = Column(String(100), nullable=True)
    # stateOrCountry field can hold US state code or foreign country code
    business_state_or_country = Column(String(10), nullable=True,
                                       index=True)  # Indexed for queries
    business_state_or_country_desc = Column(
        String(50), nullable=True)  # Description (e.g., 'AL' or 'Canada')
    business_zip = Column(String(20), nullable=True)
    # Phone number
    phone = Column(String(25),
                   nullable=True)  # Slightly longer for international etc.
    # ----------------------------

    filings = relationship(
        "Filing", backref="company")  # Keep backref if needed elsewhere

    def __repr__(self):
        return f"<Company(cik='{self.cik}', name='{self.name}')>"


class Filing(Base):
    __tablename__ = 'filings'  # Correct lowercase name matching DB
    id = Column(Integer, primary_key=True, autoincrement=True)
    cik = Column(String(10),
                 ForeignKey('companies.cik', ondelete="CASCADE"),
                 nullable=False)
    form_type = Column(String(20), nullable=False)
    filing_date = Column(Date, nullable=False)
    accession_number = Column(String(30), unique=True, nullable=False)
    primary_document_filename = Column(String(255),
                                       nullable=True)  # Correct column

    __table_args__ = (
        Index('idx_form_date', 'form_type', 'filing_date'),
        Index('idx_cik', 'cik'),
    )

    def __repr__(self):
        return (
            f"<Filing(id={self.id}, cik='{self.cik}', form_type='{self.form_type}', "
            f"filing_date='{self.filing_date}', accession_number='{self.accession_number}')>"
        )


# --- MODIFIED create_database_tables ---
def create_database_tables(engine):  # Pass engine in
    """Creates the database tables based on SQLAlchemy models."""
    if engine is None:
        logger.error("Engine required to create tables.")
        return False
    logger.info("Creating database tables if they don't exist...")
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables checked/created successfully.")
        return True
    except Exception as e:
        logger.error(f"Error creating database tables: {e}", exc_info=True)
        return False


# --- MODIFIED initialize_database ---
def initialize_database():
    """
    Checks/creates DB, creates engine and session factory, creates tables.
    Returns a tuple: (engine, session_factory) on success, or (None, None) on failure.
    """
    # global engine, SessionLocal # REMOVE GLOBALS

    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT', '3306')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')

    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]):
        logger.error("Database environment variables missing.")
        return None, None  # Return None tuple on failure

    server_url = f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/?charset=utf8mb4"
    local_engine = None  # Use local variables
    local_session_factory = None

    try:
        logger.info(f"Checking/creating database '{DB_NAME}'...")
        temp_engine = create_engine(server_url)
        with temp_engine.connect() as connection:
            with connection.begin():
                connection.execute(
                    text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        logger.info(f"Database '{DB_NAME}' checked/created.")
        temp_engine.dispose()
    except Exception as e:
        logger.error(f"Failed to check/create database '{DB_NAME}': {e}",
                     exc_info=True)
        return None, None  # Return None tuple

    DATABASE_URL = (f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@"
                    f"{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4")

    try:
        logger.info(f"Connecting to database '{DB_NAME}'...")
        local_engine = create_engine(DATABASE_URL,
                                     pool_recycle=3600,
                                     pool_size=10,
                                     max_overflow=20,
                                     pool_pre_ping=True)
        # Create the session factory
        local_session_factory = scoped_session(
            sessionmaker(autocommit=False, autoflush=False, bind=local_engine))
        logger.info("Database engine and session factory created.")

        # Create tables using the newly created engine
        if not create_database_tables(local_engine):
            # If table creation fails, maybe still return engine/session? Or fail completely?
            # Let's fail completely for now.
            logger.error("Table creation failed during initialization.")
            if local_engine:
                local_engine.dispose()  # Clean up engine if created
            return None, None

        # Return the created engine and session factory on success
        return local_engine, local_session_factory

    except Exception as e:
        logger.error(
            f"Failed to connect or setup session/tables in '{DB_NAME}': {e}",
            exc_info=True)
        if local_engine: local_engine.dispose()  # Clean up engine if created
        return None, None  # Return None tuple


# Entry point for running models.py directly
if __name__ == "__main__":
    logger.info(
        "Running models.py directly. Attempting to initialize database and tables."
    )
    if initialize_database():
        logger.info("Database and tables initialized successfully.")
    else:
        logger.error("Database initialization failed.")
        # sys.exit(1) # Optionally exit if initialization fails
