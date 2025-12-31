import os, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import polars as pl
from src.schema.semantic import SemanticPaper as Paper
from src.schema.util import pl_schema_from_pydantic, pl_df_from_pydantic_list

# --- 测试代码 ---
if __name__ == "__main__":
    # 使用你提供的其中一条真实数据进行测试
    raw_data = {
        'paperId': '7250889c52660a4a77e02c76236b2443f33b9eae',
        'externalIds': {'MAG': '2568841898', 'DOI': '10.1016/j.tics.2016.12.007', 'CorpusId': 2552560, 'PubMed': '28063661'},
        'url': 'https://www.semanticscholar.org/paper/7250889c52660a4a77e02c76236b2443f33b9eae',
        'title': 'The Distributed Nature of Working Memory',
        'venue': 'Trends in Cognitive Sciences',
        'year': 2017,
        'citationCount': 673,
        'influentialCitationCount': 37,
        'isOpenAccess': True,
        'openAccessPdf': {'url': 'https://research.vu.nl/files/272546402/The_Distributed_Nature_of_Working_Memory.pdf', 'status': 'GREEN', 'license': 'other-oa'},
        'fieldsOfStudy': ['Psychology', 'Medicine'],
        's2FieldsOfStudy': [{'category': 'Psychology', 'source': 'external'}, {'category': 'Medicine', 'source': 'external'}],
        'publicationTypes': ['Review', 'JournalArticle'],
        'publicationDate': '2017-02-01',
        'authors': [{'authorId': '2541584', 'name': 'T. Christophel'}],
        'abstract': None
    }

    
    paper = Paper(**raw_data)
    print("✅ 模型验证成功！")
    print(f"Title: {paper.title}")
    print(f"Abstract Length: {len(paper.abstract) if paper.abstract else 'No Abstract'}")
    print(f"First Author: {paper.authors[0].name}")

    # 保存示例：从 Pydantic 模型自动推导 Polars schema
    df = pl_df_from_pydantic_list([paper])
    output_path = REPO_ROOT / "playground" / "semantic_papers.parquet"
    df.write_parquet(output_path)
    print(f"✅ 已写入 Parquet: {output_path}")

