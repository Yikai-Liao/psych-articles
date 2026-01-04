from pydantic import BaseModel, Field
from typing import Optional, List, Union
from semanticscholar.Paper import Paper as S2Paper

class ExternalIds(BaseModel):
    """
    Paper external IDs (DOI, MAG, CorpusId, etc.)
    使用 Optional 因为不同论文拥有的 ID 类型不同
    """
    MAG: Optional[str] = None
    DOI: Optional[str] = None
    # CorpusId 在原始数据中可能是 int，这里允许自动转为 str，或者你可以保留为 Union[str, int]
    CorpusId: Optional[Union[str, int]] = None 
    PubMed: Optional[str] = None
    PubMedCentral: Optional[str] = None
    DBLP: Optional[str] = None
    # 允许额外的未知 ID 字段
    model_config = {"extra": "ignore"}

class OpenAccessPdf(BaseModel):
    """
    PDF link and status information
    """
    url: Optional[str] = None
    status: Optional[str] = None
    license: Optional[str] = None
    disclaimer: Optional[str] = None

class S2FieldOfStudy(BaseModel):
    category: str
    source: str

class Author(BaseModel):
    authorId: str
    name: str

class SemanticPaper(BaseModel):
    """
    Main data model for Semantic Scholar paper
    """
    paperId: str
    externalIds: ExternalIds = Field(default_factory=ExternalIds)
    url: str
    title: str
    venue: Optional[str] = None  # 并非所有论文都有 venue
    year: Optional[int] = None
    citationCount: int = 0
    influentialCitationCount: int = 0
    isOpenAccess: bool = False
    openAccessPdf: Optional[OpenAccessPdf] = None
    fieldsOfStudy: Optional[List[str]] = None
    s2FieldsOfStudy: List[S2FieldOfStudy] = Field(default_factory=list)
    publicationTypes: Optional[List[str]] = None
    publicationDate: Optional[str] = None
    authors: List[Author] = Field(default_factory=list)
    abstract: Optional[str] = None  # 注意：你的数据中 Paper [5] 的 abstract 是 None

    @classmethod
    def from_s2paper(cls, s2paper: S2Paper) -> "SemanticPaper":
        """
        Create SemanticPaper from semanticscholar.Paper instance
        """
        if s2paper is None:
            raise ValueError("s2paper cannot be None")

        external_ids = getattr(s2paper, "externalIds", None) or {}
        open_access_pdf_data = getattr(s2paper, "openAccessPdf", None)
        open_access_pdf = (
            OpenAccessPdf(**open_access_pdf_data) if open_access_pdf_data else None
        )

        s2_fields = []
        for item in getattr(s2paper, "s2FieldsOfStudy", None) or []:
            if isinstance(item, S2FieldOfStudy):
                s2_fields.append(item)
            elif isinstance(item, dict):
                s2_fields.append(S2FieldOfStudy(**item))
            else:
                category = getattr(item, "category", None)
                source = getattr(item, "source", None)
                if category is not None and source is not None:
                    s2_fields.append(S2FieldOfStudy(category=category, source=source))

        authors = []
        for item in getattr(s2paper, "authors", None) or []:
            if isinstance(item, Author):
                authors.append(item)
            else:
                author_id = getattr(item, "authorId", None)
                name = getattr(item, "name", "")
                authors.append(Author(authorId=str(author_id) if author_id is not None else "", name=name))

        publication_date = getattr(s2paper, "publicationDate", None)
        if publication_date is not None and hasattr(publication_date, "strftime"):
            publication_date = publication_date.strftime("%Y-%m-%d")

        return cls(
            paperId=getattr(s2paper, "paperId", None),
            externalIds=ExternalIds(**external_ids) if isinstance(external_ids, dict) else ExternalIds(),
            url=getattr(s2paper, "url", None),
            title=getattr(s2paper, "title", None),
            venue=getattr(s2paper, "venue", None),
            year=getattr(s2paper, "year", None),
            citationCount=getattr(s2paper, "citationCount", 0) or 0,
            influentialCitationCount=getattr(s2paper, "influentialCitationCount", 0) or 0,
            isOpenAccess=bool(getattr(s2paper, "isOpenAccess", False)),
            openAccessPdf=open_access_pdf,
            fieldsOfStudy=getattr(s2paper, "fieldsOfStudy", None),
            s2FieldsOfStudy=s2_fields,
            publicationTypes=getattr(s2paper, "publicationTypes", None),
            publicationDate=publication_date,
            authors=authors,
            abstract=getattr(s2paper, "abstract", None),
        )

    @staticmethod
    def query_fields():
        """
        The fields to request when querying Semantic Scholar API
        """
        return [
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
