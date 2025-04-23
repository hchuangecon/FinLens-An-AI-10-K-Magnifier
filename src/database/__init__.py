# src/database/__init__.py

# Expose key ORM components from the models module
from .models import Base, Company, Filing

# Expose key functions/classes for session management from the session module
from .session import initialize_database, get_session  # Expose the context manager

# Expose the concrete repository classes
from .repositories.company import CompanyRepository
from .repositories.filing import FilingRepository

# Optional: Expose the base repository if needed for type hinting or extension elsewhere
# from .repositories.base import AbstractRepository

# Define what '__all__' does if someone uses 'from src.database import *'
# Good practice for defining the public API of the package,
# though 'import *' is generally discouraged in application code.
__all__ = [
    # Models
    "Base",
    "Company",
    "Filing",
    # Session Management
    "initialize_database",
    "get_session",
    # Repositories
    "CompanyRepository",
    "FilingRepository",
]
