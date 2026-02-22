import duckdb
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

ALLOWED_FORMATS = {"CSV", "JSON", "PARQUET"}


class DuckDBClient:
    def __init__(self):
        self.con = duckdb.connect()

    def register(self, name: str, df):
        try:
            self.con.register(name, df)
        except Exception:
            logger.exception("Failed to register dataframe in DuckDB")
            raise

    def query(self, sql: str, params: list | None = None):
        try:
            if params:
                return self.con.execute(sql, params).df()
            return self.con.execute(sql).df()
        except Exception:
            logger.exception("DuckDB query execution failed")
            raise

    def export(self, sql: str, path: str, file_format: str = "csv"):
        try:
            format_upper = file_format.upper()

            if format_upper not in ALLOWED_FORMATS:
                raise ValueError(f"Invalid export format: {file_format}")

            safe_path = Path(path).resolve()

            self.con.execute(
                f"""
                COPY ({sql})
                TO '{safe_path}'
                (FORMAT {format_upper}, HEADER)
                """
            )

        except Exception:
            logger.exception("DuckDB export failed")
            raise