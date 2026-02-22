import pandas as pd
from .cleaning import extract_year, clean_budget


def build_dataset(df_movies: pd.DataFrame, df_details: pd.DataFrame) -> pd.DataFrame:
    df_details = df_details.copy()

    df_details["year"] = df_details["release_dates"].apply(extract_year)
    df_details["budget_usd"] = df_details["budget"].apply(clean_budget)

    df_merged = df_movies.merge(
        df_details[["detail_url", "year", "budget", "budget_usd"]],
        on="detail_url",
        how="inner"
    )

    return df_merged