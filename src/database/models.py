# src/database/models.py
import logging
import sys
# from dotenv import load_dotenv # Removed - Handled by settings.py
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Index, Boolean
# from sqlalchemy.dialects.mysql import TEXT # Only needed if you use TEXT type

# Basic Logging Setup (Can potentially be centralized later)
logger = logging.getLogger(
    'sec_pipeline.db')  # Keep logger name consistent for now
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)
                  ])  # Use stdout for simplicity now

# Base class for declarative models
Base = declarative_base()


# Define Database Models (Company, Filing - unchanged model definitions)
class Company(Base):
    __tablename__ = 'companies'
    cik = Column(String(10), primary_key=True, unique=True, nullable=False)
    name = Column(String(255), nullable=False, index=True)
    sic = Column(String(4), nullable=True, index=True)

    # --- NEW REQUESTED FIELDS ---
    entity_type = Column(String(50), nullable=True,
                         index=True)  # e.g., 'operating'
    sic_description = Column(String(255), nullable=True)
    insider_trade_owner = Column(Boolean, nullable=True)
    insider_trade_issuer = Column(Boolean, nullable=True)

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
        "Filing", back_populates="company")  # Use back_populates for clarity

    def __repr__(self):
        return f"<Company(cik='{self.cik}', name='{self.name}')>"


class Filing(Base):
    __tablename__ = 'filings'
    id = Column(Integer, primary_key=True, autoincrement=True)
    cik = Column(String(10),
                 ForeignKey('companies.cik', ondelete="CASCADE"),
                 nullable=False)
    form_type = Column(String(20), nullable=False)
    filing_date = Column(Date, nullable=False)
    accession_number = Column(String(30), unique=True, nullable=False)
    primary_document_filename = Column(String(255), nullable=True)

    company = relationship(
        "Company", back_populates="filings")  # Define relationship back

    __table_args__ = (
        Index('idx_form_date', 'form_type', 'filing_date'),
        Index('idx_cik', 'cik'),
        Index('idx_accession',
              'accession_number'),  # Added index for accession number lookups
    )

    def __repr__(self):
        return (
            f"<Filing(id={self.id}, cik='{self.cik}', form_type='{self.form_type}', "
            f"filing_date='{self.filing_date}', accession_number='{self.accession_number}')>"
        )
