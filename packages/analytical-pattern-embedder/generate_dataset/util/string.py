import re


def snake(text: str) -> str:
    """"Sql Query!" -> "sql_query"."""
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def to_pascal_case(text: str) -> str:
    """Words capitalised and joined by underscores: "sql query" -> "Sql_Query"."""
    words = [word for word in re.split(r"[^A-Za-z0-9]+", text) if word]
    return "_".join(word[:1].upper() + word[1:] for word in words)


def normalize(text: str) -> str:
    """Lowercase words separated by single spaces, to compare two sentences."""
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
