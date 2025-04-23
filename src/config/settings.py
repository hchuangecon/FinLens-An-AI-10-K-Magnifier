# finlens/connfig/settings.py

from typing import Optional, List, Set
from pydantic import BaseSettings, Field, validator
import os
from pathlib import Path


class DatabaseSettings(BaseSettings):
    host: str = Field(..., env="DB_HOST")
    port: int = Field(3306, env="DB_PORT")
    user: str = Field(..., env="DB_USER")
    password: str = Field(..., env="DB_PASSWORD")
    name: str = Field(..., env="DB_NAME")

    class Config:
        env_file = ".env"


class SECAPISettings(BaseSettings):
    user_agent: str = Field(..., env="SEC_USER_AGENT")
    base_url: str = "https://www.sec.gov"
    api_base_url: str = "https://data.sec.gov"
    submissions_api_base: str = "https://data.sec.gov/submissions/CIK"
    edgar_archive_base: str = "https://www.sec.gov/Archives/edgar/data"
    submissions_bulk_url: str = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    rate_limit: float = 0.11  # seconds between requests

    @validator('user_agent')
    def validate_user_agent(cls, v):
        if not v:
            raise ValueError("SEC_USER_AGENT environment variable must be set")
        return v

    class Config:
        env_file = ".env"
        case_sensitive = False


class FilingFilterSettings(BaseSettings):
    excluded_sic_codes: List[str] = ["6189", "6722", "6726"]

    class Config:
        env_file = ".env"
        case_sensitive = False


class PipelineSettings(BaseSettings):
    data_path: Path = Field(Path("data"), env="DATA_STORAGE_PATH")
    bulk_workers: Optional[int] = Field(None, env="BULK_WORKERS")
    download_threads: int = Field(10, env="DOWNLOAD_THREADS")

    # Default number of past days to check for incremental updates
    incremental_days_to_check: int = Field(31, env="INCREMENTAL_DAYS_CHECK")
    # Default set of target forms for document downloading and potentially backfill filtering
    # Use comma-separated string in .env, convert to set here
    target_primary_doc_forms: Set[str] = Field({"10-K", "10-K/A"},
                                               env="TARGET_DOC_FORMS")

    # Default set of forms to filter for during historical backfill (can be overridden)
    # If empty or not set, backfill might process all forms (as per current logic)
    # Let's default to the same as primary doc forms for consistency
    backfill_target_forms: Optional[Set[str]] = Field(
        None, env="BACKFILL_TARGET_FORMS")

    # Document storage subdirectory name
    document_subdir: str = Field("filing_documents", env="DOC_SUBDIR")

    @validator('bulk_workers', pre=True, always=True)
    def set_bulk_workers(cls, v):
        if v is None:
            # Default to CPU count - 1, but minimum of 1
            return max(1, os.cpu_count() - 1) if os.cpu_count() else 4
        return v

    @validator('data_path', pre=True)
    def validate_data_path(cls, v):
        if isinstance(v, str):
            return Path(v)
        return v

    @validator('target_primary_doc_forms', 'backfill_target_forms', pre=True)
    def parse_form_set(cls, v):
        if isinstance(v, str):
            # Split comma-separated string, strip whitespace, convert to upper, filter empty
            forms = {
                form.strip().upper()
                for form in v.split(',') if form.strip()
            }
            return forms if forms else None  # Return None if empty after processing
        if isinstance(v, (set, list, tuple)):
            return {
                str(form).strip().upper()
                for form in v if str(form).strip()
            } or None
        if v is None:  # Allow None for optional backfill_target_forms
            return None
        return v  # Assume already a set if not str/list/tuple/None

    class Config:
        env_file = ".env"
        case_sensitive = False


class AppSettings(BaseSettings):
    database: DatabaseSettings = DatabaseSettings()
    sec_api: SECAPISettings = SECAPISettings()
    pipeline: PipelineSettings = PipelineSettings()
    filing_filter: FilingFilterSettings = FilingFilterSettings()

    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton pattern for settings
_settings = None


def get_settings() -> AppSettings:
    global _settings
    if _settings is None:
        _settings = AppSettings()
    return _settings
