import polars as pl
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

# 1. Define keywords for exclusion
# Use word boundaries (\b) to avoid misidentification (e.g., 'Canada' containing 'CDA')
# '(?i)' prefix can make specific parts case-insensitive if needed,
# but here we separate clinical terms from technical abbreviations.
technical_terms = [
    r"\bCDA\b",  # Case-sensitive match for abbreviation
    r"\bfMRI\b",
    r"\bEEG\b",
    r"\bMEG\b",
    r"\biEEG\b",
    r"\bfNIRS\b",
    r"\bTMS\b",
    r"\btDCS\b",
    r"\btACS\b",
    "contralateral delay activity",
    "Alpha power suppression",
]

disease_terms = [
    r"\bADHD\b",
    r"\bASD\b",
    r"(?i)Alzheimer",
    r"(?i)Schizo",
]

child_terms = [
    r"(?i)\bChild\b",
    r"(?i)teenage",
]

# Combine all patterns with OR operator
exclusion_pattern = "|".join(technical_terms + disease_terms + child_terms)


def base_query(file_path: str) -> pl.LazyFrame:
    """
    Base query with access/abstract filters applied.
    """
    return pl.scan_parquet(file_path).filter(
        pl.col("isOpenAccess") == True,
        pl.col("abstract").is_not_null(),
    )


def filter_parquet_data(file_path: str) -> pl.DataFrame:
    """
    Scans a parquet file and filters records based on access status,
    abstract availability, and keyword exclusion.
    """
    # Use scan_parquet for lazy evaluation (better performance)
    q = base_query(file_path).filter(
        # Filter out abstracts containing the pattern
        # Note: contains() with a regex is powerfull.
        # Use literal=False to enable regex.
        ~pl.col("abstract").str.contains(exclusion_pattern, literal=False)
    )

    return q.collect()


def term_counts(file_path: str, terms: list[str]) -> dict[str, int]:
    base = base_query(file_path)
    count_exprs = [
        pl.col("abstract").str.contains(term, literal=False).sum().alias(f"t{i}")
        for i, term in enumerate(terms)
    ]
    counts_row = base.select(count_exprs).collect().row(0)
    return {terms[i]: int(counts_row[i]) for i in range(len(terms))}


def term_example_title(file_path: str, term: str) -> str | None:
    base = base_query(file_path)
    sample = (
        base.filter(pl.col("abstract").str.contains(term, literal=False))
        .select(["title"])
        .limit(1)
        .collect()
    )
    if sample.is_empty():
        return None
    return sample.row(0)[0]


def compact_exclusion_table(
    file_path: str, terms: list[str], group: str
) -> pl.DataFrame:
    counts = term_counts(file_path, terms)
    base = base_query(file_path)
    total_base = int(base.select(pl.len()).collect().item())
    rows = []
    for term in terms:
        count = counts.get(term, 0)
        ratio = (count / total_base * 100.0) if total_base else 0.0
        title = term_example_title(file_path, term) or "N/A"
        rows.append(
            {
                "term": term,
                "group": group,
                "count": count,
                "pct_base": round(ratio, 2),
                "example_title": title,
            }
        )
    return pl.DataFrame(rows)


def open_access_stats(file_path: str) -> dict[str, int]:
    q = pl.scan_parquet(file_path)
    total_rows = int(q.select(pl.len()).collect().item())
    non_open_access = int(
        q.filter(pl.col("isOpenAccess") == False).select(pl.len()).collect().item()
    )
    open_no_abstract = int(
        q.filter(pl.col("isOpenAccess") == True, pl.col("abstract").is_null())
        .select(pl.len())
        .collect()
        .item()
    )
    return {
        "total_rows": total_rows,
        "non_open_access": non_open_access,
        "open_no_abstract": open_no_abstract,
    }


def report_exclusions(file_path: str) -> None:
    terms = technical_terms + disease_terms + child_terms
    base = base_query(file_path)
    total_base = int(base.select(pl.len()).collect().item())
    total_excluded = int(
        base.filter(pl.col("abstract").str.contains(exclusion_pattern, literal=False))
        .select(pl.len())
        .collect()
        .item()
    )
    remaining = total_base - total_excluded
    access_stats = open_access_stats(file_path)

    print(f"Total rows: {access_stats['total_rows']}")
    print(f"Non-open access rows: {access_stats['non_open_access']}")
    print(f"Open access but no abstract rows: {access_stats['open_no_abstract']}")
    print(f"Base rows (open access + abstract): {total_base}")
    print(f"Excluded rows (any term): {total_excluded}")
    print(f"Remaining rows: {remaining}")
    print("-" * 80)
    print("Compact exclusion table (counts can overlap across terms):")
    technical_table = compact_exclusion_table(file_path, technical_terms, "technical")
    disease_table = compact_exclusion_table(file_path, disease_terms, "disease")
    child_table = compact_exclusion_table(file_path, child_terms, "child")
    compact_table = pl.concat([technical_table, disease_table, child_table])
    print(compact_table)
    print("-" * 80)


# Execute
data_path = str(REPO_ROOT / "assets" / "semantic_papers_all.parquet")
report_exclusions(data_path)

filtered_data = filter_parquet_data(data_path)
filtered_data.write_parquet(REPO_ROOT / "assets" / "semantic_papers_filtered.parquet")
print(
    f"\nSaved {filtered_data.height} filtered rows to semantic_papers_filtered.parquet"
)
