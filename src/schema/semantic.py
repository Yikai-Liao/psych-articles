from pydantic import BaseModel, Field
from typing import Optional, List, Union

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
    url: str
    status: str
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

    @property
    def query_fields(self):
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