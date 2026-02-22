import duckdb
import logging

logger = logging.getLogger(__name__)


class DuckDBClient:
    def __init__(self):
        self.con = duckdb.connect()

    def register(self, name: str, df):
        try:
            self.con.register(name, df)
        except Exception:
            logger.exception("Failed to register dataframe in DuckDB")
            raise

    def query(self, sql: str):
        try:
            return self.con.execute(sql).df()
        except Exception:
            logger.exception("Query execution failed")
            raise

    def export_csv(self, sql: str, path: str):
        try:
            self.con.execute(
                f"""
                COPY ({sql})
                TO '{path}'
                (FORMAT CSV, HEADER)
                """
            )
        except Exception:
            logger.exception("Failed to export CSV using DuckDB")
            raise