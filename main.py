from pathlib import Path
import logging
import pandas as pd
from src.transform import build_dataset
from src.duckdb_client import DuckDBClient

BASE_DIR = Path(__file__).resolve().parent

RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "output"

MOVIES_PATH = RAW_DIR / "movies.json"
DETAILS_PATH = RAW_DIR / "movie-detail.json"

OUTPUT_FILE = OUTPUT_DIR / "oscars_post_1955.csv"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def load_movies(path: Path) -> pd.DataFrame:
    if not path.exists():
        logger.error(f"Movies file not found: {path}")
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_json(path, lines=True)


def load_movie_details(path: Path) -> pd.DataFrame:
    if not path.exists():
        logger.error(f"Details file not found: {path}")
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_json(path, lines=True)


def main():
    try:
        logger.info("Starting Oscar Movies ETL pipeline")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("Loading raw datasets")
        df_movies = load_movies(MOVIES_PATH)
        df_details = load_movie_details(DETAILS_PATH)

        logger.info("Building unified dataset")
        df_dataset = build_dataset(df_movies, df_details)

        logger.info("Registering dataset in DuckDB")
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

        logger.info("Executing filtering query and exporting CSV")
        client.export_csv(query, str(OUTPUT_FILE))

        logger.info(f"Pipeline completed successfully. Output: {OUTPUT_FILE}")

    except Exception:
        logger.exception("Pipeline execution failed")
        raise


if __name__ == "__main__":
    main()