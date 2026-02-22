# Oscar Movies ETL

This project implements a modular ETL pipeline in Python to process Oscar-nominated movie data and generate a filtered CSV file using DuckDB.

The final output contains Oscar-winning movies released after 1955 with a minimum budget of 15 million USD.

# Requirements

- Python 3.8+
- pip

# Setup Instructions

Clone the repository or extract the ZIP file.

Create a virtual environment:

```
python -m venv venv
```
Activate the virtual environment (Windows):
```
venv\Scripts\activate
```
Install dependencies:
```
pip install -r requirements.txt
```
# Running the Pipeline

Execute:
```
python main.py
```
The script will:

- Load raw JSON files from `data/raw/`
- Clean and transform the dataset
- Execute filtering logic using DuckDB
- Export the final CSV file to:

data/output/oscars_post_1955.csv

# Output Schema

The generated CSV contains the following columns:

- film
- year
- wikipedia_url
- original_budget
- budget_usd

# Assumptions

- The first 4-digit year found in `release_dates` represents the film release year.
- Currency conversion rates are fixed:
  - £1 ≈ 1.3 USD
  - €1 ≈ 1.1 USD
- Budgets containing the word "million" are scaled by 1,000,000.
- Budget ranges use the greater value.
- Missing or invalid budgets are treated as 0.

# Notes

Running the script will regenerate the output file.
All transformation details and design decisions are documented in EXPLAIN.md.