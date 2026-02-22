import re


def extract_year(release_date: str) -> int | None:
    if not release_date:
        return None

    match = re.search(r"\b(19|20)\d{2}\b", release_date)
    if match:
        return int(match.group(0))

    return None


def remove_references(value: str) -> str:
    return re.sub(r"\[\s*\d+\s*\]", "", value)


def extract_range(value: str) -> str:
    if "-" in value:
        parts = value.split("-")
        return parts[-1]
    return value


def normalize_currency(value: str) -> tuple[str, float]:
    if "£" in value:
        return value.replace("£", ""), 1.3
    if "€" in value:
        return value.replace("€", ""), 1.1
    if "US$" in value:
        return value.replace("US$", ""), 1.0
    if "$" in value:
        return value.replace("$", ""), 1.0
    return value, 1.0


def parse_numeric(value: str) -> float:
    value = value.replace(",", "").strip()
    match = re.search(r"\d+(\.\d+)?", value)
    if match:
        return float(match.group(0))
    return 0.0


def clean_budget(budget: str | None) -> int:
    if not isinstance(budget, str):
        return 0

    original_value = budget.lower()

    value = remove_references(budget)
    value = extract_range(value)
    value, rate = normalize_currency(value)
    number = parse_numeric(value)

    if "million" in original_value:
        number *= 1_000_000

    return int(number * rate)