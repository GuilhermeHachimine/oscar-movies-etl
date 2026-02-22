# Overview

This project implements a modular ETL pipeline to clean, transform, and analyze Oscar-nominated movie data. The objective is to export a CSV file containing Oscar-winning movies released after 1955 with a minimum budget of 15 million USD.

The solution follows a modular, class-based structure with clear separation of responsibilities.

# Architecture

The project is structured into distinct layers:

- `cleaning.py` handles column-level data cleaning.
- `transform.py` constructs the unified dataset and applies transformations.
- `duckdb_client.py` encapsulates database interaction logic.
- `main.py` orchestrates the pipeline execution.

Raw data is stored under `data/raw/`, and generated outputs are written to `data/output/`.

This design enforces separation of concerns and improves maintainability, readability, and extensibility.

# Data Cleaning

Year extraction is performed using a regular expression applied to the `release_dates` field. The first four-digit year (19xx or 20xx) found in the string is considered the release year of the film. If no year is found, `None` is returned. In this dataset, all records contained a valid year.

Budget cleaning includes handling missing values, reference markers, currency normalization, and ranges. Null or non-string values are converted to 0. Reference markers such as `[1]` are removed using regular expressions. Currency symbols are normalized with fixed conversion rates: USD uses a rate of 1.0, GBP (£) uses 1.3, and EUR (€) uses 1.1. Budget ranges such as `$16.5-18 million` use the greater value in the range. The presence of the word `million` explicitly triggers scaling by 1,000,000. The final converted value is returned as an integer in USD.

The original raw budget string is preserved in the final output as `original_budget`.

# Join Strategy

An inner join is performed between `movies` and `movie-detail` datasets using `detail_url` as the join key.

Records without matching detailed metadata are excluded because they do not contain the necessary fields for filtering (year and budget). After the join, the unified dataset contains 516 records.

# Filtering Logic

The final dataset includes records that satisfy all of the following conditions:

- The film won an Oscar.
- The release year is greater than 1955.
- The budget in USD is greater than or equal to 15,000,000.

Intermediate counts were validated to ensure correctness:

- Total merged dataset: 516 records
- Oscar winners: 87 records
- Year greater than 1955: 317 records
- Budget greater than or equal to 15M USD: 145 records
- Final intersection of all conditions: 32 records

These counts confirm logical consistency of the filtering process.

# DuckDB Integration

DuckDB is used to register the transformed dataset as a virtual table, execute the SQL filtering query, and export the final result using the `COPY` command.

This approach ensures proper SQL-based interaction with the data and aligns with the assignment requirement to integrate a third-party library for querying and exporting data.

Data transformation is performed in Pandas, while querying and exporting are handled through DuckDB to maintain separation of processing and analytical layers.

# Assumptions

The first four-digit year in the `release_dates` field represents the release year of the film. Currency conversion rates are fixed and not dynamically retrieved. Budgets explicitly containing the word `million` are scaled accordingly. Missing or invalid budget values are treated as 0.

# Possible Improvements

With additional time, the solution could be extended with unit tests for cleaning functions, structured logging, configurable currency rates, more robust currency parsing patterns, and schema validation before processing.