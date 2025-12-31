import os, sys
from dotenv import load_dotenv
from semanticscholar import SemanticScholar, PaginatedResults

load_dotenv()
S2_API_KEY = os.getenv("S2_API_KEY")
assert S2_API_KEY is not None, "S2_API_KEY environment variable not set"


sch = SemanticScholar(
    api_key=S2_API_KEY
)
fields_to_return = [
    'paperId', 
    'title', 
    'abstract', 
    'venue', 
    'year', 
    'publicationDate', 
    'citationCount', 
    'influentialCitationCount', 
    'isOpenAccess', 
    'openAccessPdf', 
    'fieldsOfStudy', 
    's2FieldsOfStudy', 
    'authors', 
    'publicationTypes', 
    'externalIds', 
    'url'
]

results = sch.search_paper(
    query="working memory",
    fields_of_study=['Psychology'],
    publication_date_or_year='2016:2026',  # 格式：YYYY-MM-DD:YYYY-MM-DD 或 YYYY-
    sort='citationCount:desc',         # 格式：field:order
    open_access_pdf=True,              # 仅返回有公开PDF的论文
    bulk=True,                         # 开启 Bulk 检索模式
    fields=fields_to_return,           # 显式指定需要返回的字段
)

# 遍历结果 (Bulk 模式返回的是一个生成器/迭代器)
# 注意：results 这里通常是 PaginatedResults 对象，可以直接迭代
print(f"Total estimated matches: {results.total}")

for i, paper in enumerate(results):
    print(f"[{i+1}] {paper.title} (Citations: {paper.citationCount})")
    print(paper)
    
    # 演示仅打印前5条
    if i >= 4:
        break