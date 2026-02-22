import duckdb


class DuckDBClient:
    def __init__(self):
        self.con = duckdb.connect()

    def register(self, name: str, df):
        self.con.register(name, df)

    def query(self, sql: str):
        return self.con.execute(sql).df()

    def export_csv(self, sql: str, path: str):
        self.con.execute(
            f"""
            COPY ({sql})
            TO '{path}'
            (FORMAT CSV, HEADER)
            """
        )