import os, sys
from dotenv import load_dotenv
from pathlib import Path
from semanticscholar.PaginatedResults import PaginatedResults

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from src.api.semantic import RateLimitedSemanticScholar
from src.schema.semantic import SemanticPaper
from src.schema.util import pl_df_from_pydantic_list
from tqdm import tqdm


load_dotenv()
S2_API_KEY = os.getenv("S2_API_KEY")
assert S2_API_KEY is not None, "S2_API_KEY environment variable not set"


sch = RateLimitedSemanticScholar(
    api_key=S2_API_KEY
)

required_terms = [
    "working memory",
]


query = "working memory"

results = sch.search_paper(
    query=query,
    fields_of_study=['Psychology'],
    publication_date_or_year='2016:2026',  # 格式：YYYY-MM-DD:YYYY-MM-DD 或 YYYY-
    sort='citationCount:desc',             # 格式：field:order
    # open_access_pdf=True,                  # 仅返回有公开PDF的论文
    bulk=True,                             # 开启 Bulk 检索模式
    fields=SemanticPaper.query_fields(),   # 显式指定需要返回的字段
    limit=1000,                             # 每页面面返回的结果数，bulk 模式下最大可设为 1000，不是总数限制
)

assert isinstance(results, PaginatedResults), "Expected PaginatedResults from bulk search"

papers = []
for i, s2paper in tqdm(enumerate(results, start=1)):
    papers.append(SemanticPaper.from_s2paper(s2paper))

table = pl_df_from_pydantic_list(papers)

print(f"Retrieved {table.height} papers.")
print(table.head(5))

output_path = REPO_ROOT / "playground" / "semantic_papers_all.parquet"

table.write_parquet(output_path)
print(f"Saved papers to {output_path}")
 
