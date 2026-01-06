import os
import polars as pl
import sys
from pathlib import Path
from urllib.parse import urlparse
import time
from typing import Dict, List
from collections import defaultdict



REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.downloader import DirectOrCloudflareStrategy


DOWNLOAD_DIR = REPO_ROOT / "download"
DOWNLOAD_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

PLAYWRIGHT_HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "1") != "0"
PLAYWRIGHT_WAIT_SECONDS = int(os.getenv("PLAYWRIGHT_WAIT_SECONDS", "60"))
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "120"))

STRATEGY = DirectOrCloudflareStrategy(
    headers=HEADERS,
    timeout=REQUEST_TIMEOUT_SECONDS,
)


def get_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(url)
    return parsed.netloc


def download_pdf(paper_id: str, pdf_url: str, title: str) -> tuple[bool, str]:
    """Download PDF from URL and save to download directory."""
    filename = f"{paper_id}.pdf"
    filepath = DOWNLOAD_DIR / filename

    result = STRATEGY.download(pdf_url, filepath)
    return result.success, result.message


def sample_and_test_download(file_path: str, samples_per_domain: int = 3) -> None:
    """Sample and download PDFs by domain for testing."""
    df = pl.read_parquet(file_path)
    pdf_df = df.with_columns(
        pl.col("openAccessPdf").struct.field("url").alias("pdf_url")
    ).filter(pl.col("pdf_url").is_not_null())

    pdf_df = pdf_df.with_columns(
        pl.col("pdf_url")
        .map_elements(get_domain, return_dtype=pl.String)
        .alias("domain")
    )

    total = pdf_df.height
    print(f"Total papers with PDF URLs: {total}")

    domains = pdf_df.select("domain").unique().to_series().to_list()
    print(f"Total domains: {len(domains)}")
    print(f"Top 10 domains by paper count:")

    domain_counts = (
        pdf_df.group_by("domain")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    print(domain_counts.head(10))
    print("-" * 80)

    results: Dict[str, List[Dict]] = {}
    success_count = 0
    failure_count = 0

    # for domain in domains:
    #     domain_df = pdf_df.filter(pl.col("domain") == domain).limit(samples_per_domain)
    #     samples = []
    
    samples = defaultdict(list)
    for row in pdf_df.limit(3).iter_rows(named=True):
        domain = row["domain"]
        paper_id = row["paperId"]
        pdf_url = row["pdf_url"]
        title = (
            row["title"][:80] + "..." if len(row["title"]) > 80 else row["title"]
        )

        print(f"Testing {row['domain']}: {paper_id}")
        print(f"  URL: {pdf_url}")
        success, message = download_pdf(paper_id, pdf_url, title)

        if success:
            success_count += 1
            print(f"  ✓ {message}")
        else:
            failure_count += 1
            print(f"  ✗ {message}")

        samples[domain].append(
            {
                "paper_id": paper_id,
                "title": title,
                "url": pdf_url,
                "success": success,
                "message": message,
            }
        )

    print(f"\nSummary:")
    print(f"  Total download attempts: {success_count + failure_count}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {failure_count}")

    domains_with_failures = [
        d for d, samples in results.items() if any(not s["success"] for s in samples)
    ]
    if domains_with_failures:
        print(f"\nDomains with failures: {len(domains_with_failures)}")
        for domain in domains_with_failures:
            failures = [s for s in results[domain] if not s["success"]]
            print(f"  {domain}: {len(failures)} failed")


if __name__ == "__main__":
    data_path = str(REPO_ROOT / "assets" / "semantic_papers_filtered.parquet")
    sample_and_test_download(data_path, samples_per_domain=3)
