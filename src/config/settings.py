# src/config/settings.py

# --- Standard Library Imports ---
import logging
import os
from pathlib import Path
from typing import Optional, List, Set, Any

# --- Pydantic V2 Imports ---
from pydantic import Field, field_validator, model_validator, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


# --- Database Settings ---
class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra='ignore')
    host: str = Field(..., alias="DB_HOST")
    port: int = Field(3306, alias="DB_PORT")
    user: str = Field(..., alias="DB_USER")
    password: str = Field(..., alias="DB_PASSWORD")
    name: str = Field(..., alias="DB_NAME")


# --- SEC API Settings ---
class SECAPISettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env",
                                      case_sensitive=False,
                                      extra='ignore')
    user_agent: str = Field(..., alias="SEC_USER_AGENT")
    base_url: str = "https://www.sec.gov"
    api_base_url: str = "https://data.sec.gov"
    submissions_api_base: str = "https://data.sec.gov/submissions/CIK"
    edgar_archive_base: str = "https://www.sec.gov/Archives/edgar/data"
    submissions_bulk_url: str = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    rate_limit: float = 0.11

    @field_validator('user_agent')
    @classmethod
    def validate_user_agent(cls, v: str) -> str:
        if not v or not isinstance(v, str) or '@' not in v or '.' not in v:
            raise ValueError(
                "SEC_USER_AGENT environment variable must be set and look like 'Org Contact email@example.com'"
            )
        return v


# --- Filing Filter Settings ---
class FilingFilterSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env",
                                      case_sensitive=False,
                                      extra='ignore')
    excluded_sic_codes: List[str] = ["6189", "6722", "6726"]


# --- Pipeline Settings ---
class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env",
                                      case_sensitive=False,
                                      extra='ignore')
    data_path: Path = Field(Path("data"), alias="DATA_STORAGE_PATH")
    bulk_workers: Optional[int] = Field(None, alias="BULK_WORKERS")
    download_threads: int = Field(10, alias="DOWNLOAD_THREADS")
    incremental_days_to_check: int = Field(31, alias="INCREMENTAL_DAYS_CHECK")
    target_primary_doc_forms: Set[str] = Field({"10-K", "10-K/A"},
                                               alias="TARGET_DOC_FORMS")
    backfill_target_forms: Optional[Set[str]] = Field(
        None, alias="BACKFILL_TARGET_FORMS")
    document_subdir: str = Field("filing_documents", alias="DOC_SUBDIR")
    bulk_ingest_file_chunk_size: int = Field(100000, alias="BULK_CHUNK_SIZE")

    @model_validator(mode='before')
    @classmethod
    def set_defaults_based_on_environment(cls, values: Any) -> Any:
        if isinstance(values, dict):
            if values.get('bulk_workers') is None:
                cpu_count = os.cpu_count()
                # Set default directly in values dict
                values['bulk_workers'] = max(1, cpu_count -
                                             1) if cpu_count else 4
                # Logging might be too early here if config not fully loaded
                # print(f"DEBUG: Setting default bulk_workers to {values['bulk_workers']}")
        return values

    @field_validator('data_path', mode='before')
    @classmethod
    def parse_data_path(cls, v: Any) -> Path:
        if isinstance(v, str): return Path(v)
        if isinstance(v, Path): return v
        return v  # Let Pydantic handle other types

    @field_validator('target_primary_doc_forms',
                     'backfill_target_forms',
                     mode='before')
    @classmethod
    def parse_form_set(cls, v: Any) -> Optional[Set[str]]:
        if isinstance(v, str):
            forms = {
                form.strip().upper()
                for form in v.split(',') if form.strip()
            }
            return forms if forms else None
        if isinstance(v, (set, list, tuple)):
            return {
                str(form).strip().upper()
                for form in v if str(form).strip()
            } or None
        if v is None: return None
        return v  # Let Pydantic handle other types


# --- Main App Settings ---
class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env",
                                      env_nested_delimiter='_',
                                      case_sensitive=False,
                                      extra='ignore')
    database: DatabaseSettings = DatabaseSettings()
    sec_api: SECAPISettings = SECAPISettings()
    pipeline: PipelineSettings = PipelineSettings()
    filing_filter: FilingFilterSettings = FilingFilterSettings()


# --- Singleton Pattern ---
_settings: Optional[AppSettings] = None
# Get logger instance *after* potential basicConfig call below
logger = logging.getLogger(__name__)  # Use module logger


def get_settings() -> AppSettings:
    """Loads and returns the application settings singleton."""
    global _settings
    if _settings is None:
        try:
            _settings = AppSettings()
            # Use the logger instance defined above
            logger.info("Application settings loaded successfully.")
        except Exception as e:
            # Use root logger if module logger isn't configured yet
            logging.critical(f"FATAL: Failed to load AppSettings: {e}",
                             exc_info=True)
            raise RuntimeError(
                f"Could not load application settings: {e}") from e
    return _settings


# Add basic config only if no handlers are configured (prevents overriding setup_logging)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.WARNING)  # Use WARNING to avoid too much noise initially
    logger.info("Basic logging configured in settings.py (WARN level)."
                )  # Log this setup
