from pathlib import Path
import pandas as pd
from src.transform import build_dataset
from src.duckdb_client import DuckDBClient

BASE_DIR = Path(__file__).resolve().parent

RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "output"

MOVIES_PATH = RAW_DIR / "movies.json"
DETAILS_PATH = RAW_DIR / "movie-detail.json"

OUTPUT_FILE = OUTPUT_DIR / "oscars_post_1955.csv"


def load_movies(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def load_movie_details(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df_movies = load_movies(MOVIES_PATH)
    df_details = load_movie_details(DETAILS_PATH)

    df_dataset = build_dataset(df_movies, df_details)

    client = DuckDBClient()
    client.register("movies", df_dataset)

    query = """
        SELECT
            film,
            year,
            wiki_url AS wikipedia_url,
            budget AS original_budget,
            budget_usd
        FROM movies
        WHERE winner = TRUE
          AND year > 1955
          AND budget_usd >= 15000000
    """

    client.export_csv(query, str(OUTPUT_FILE))


if __name__ == "__main__":
    main()