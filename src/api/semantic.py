import asyncio
import re
import warnings

from typing import List, Union
from semanticscholar import SemanticScholar, AsyncSemanticScholar
from semanticscholar.ApiRequester import ApiRequester
from semanticscholar.PaginatedResults import PaginatedResults
from semanticscholar.Paper import Paper
# from loguru import logger


class RateLimitedApiRequester(ApiRequester):
    def __init__(self, req_rate_limit: float, timeout: float, retry: bool = True) -> None:
        """
        An ApiRequester that enforces a rate limit on requests.
        :param float req_rate_limit: Maximum number of requests per second.
        """
        super().__init__(timeout=timeout, retry=retry)
        self._req_rate_limit = req_rate_limit
        self._min_interval = 1.0 / req_rate_limit
        self._last_request_time = None

    async def get_data_async(
        self,
        url: str,
        parameters: str,
        headers: dict,
        payload: dict = None
    ) -> Union[dict, List[dict]]:
        now = asyncio.get_event_loop().time()
        
        if (
            (self._last_request_time is not None)
            and ((elapsed := now - self._last_request_time) < self._min_interval)
        ):
            wait_time = self._min_interval - elapsed
            # logger.debug(f"Rate limiting: waiting {wait_time:.3f} seconds before next request.")
            await asyncio.sleep(wait_time) # Enforce rate limit
        
        response = await super().get_data_async(
            url=url,
            parameters=parameters,
            headers=headers,
            payload=payload
        )
        self._last_request_time = asyncio.get_event_loop().time()
        return response
        

class RateLimitedAsyncSemanticScholar(AsyncSemanticScholar):
    def __init__(
        self,
        req_rate_limit: float = 1,
        **kwargs,

    ) -> None:
        super().__init__(**kwargs)
        self._requester = RateLimitedApiRequester(
            req_rate_limit=req_rate_limit,
            timeout=self._timeout,
            retry=self._retry,
        )

    async def search_paper(
        self,
        query: str,
        year: str = None,
        publication_types: list = None,
        open_access_pdf: bool = None,
        venue: list = None,
        fields_of_study: list = None,
        fields: list = None,
        publication_date_or_year: str = None,
        min_citation_count: int = None,
        limit: int = 100,
        bulk: bool = False,
        sort: str = None,
        match_title: bool = False,
    ) -> Union[PaginatedResults, Paper]:
        # Bulk mode allows up to 1000 results per request
        max_limit = 1000 if bulk else 100
        if limit < 1 or limit > max_limit:
            raise ValueError(
                f"The limit parameter must be between 1 and {max_limit} inclusive."
            )

        if not fields:
            fields = Paper.SEARCH_FIELDS

        base_url = self.api_url + self.BASE_PATH_GRAPH
        url = f"{base_url}/paper/search"

        if bulk:
            url += "/bulk"
            if sort:
                query += f"&sort={sort}"
        elif sort:
            warnings.warn("The sort parameter is only used when bulk=True.")

        if match_title:
            url += "/match"
            if bulk:
                raise ValueError(
                    "The match_title parameter is not allowed when bulk=True."
                )

        query += f"&year={year}" if year else ""

        if publication_types:
            publication_types = ",".join(publication_types)
            query += f"&publicationTypes={publication_types}"

        query += "&openAccessPdf" if open_access_pdf else ""

        if venue:
            venue = ",".join(venue)
            query += f"&venue={venue}"

        if fields_of_study:
            fields_of_study = ",".join(fields_of_study)
            query += f"&fieldsOfStudy={fields_of_study}"

        if publication_date_or_year:
            single_date_regex = r"\d{4}(-\d{2}(-\d{2})?)?"
            full_regex = r"^({0})?(:({0})?)?$".format(single_date_regex)
            if not bool(re.fullmatch(full_regex, publication_date_or_year)):
                raise ValueError(
                    "The publication_date_or_year parameter must be in the "
                    "format <start_date>:<end_date>, where dates are in the "
                    "format YYYY-MM-DD, YYYY-MM, or YYYY."
                )
            query += f"&publicationDateOrYear={publication_date_or_year}"

        if min_citation_count:
            query += f"&minCitationCount={min_citation_count}"

        max_results = 10000000 if bulk else 1000

        results = await PaginatedResults.create(
            self._requester,
            Paper,
            url,
            query,
            fields,
            limit,
            self.auth_header,
            max_results=max_results,
        )

        return results if not match_title else results[0]


class RateLimitedSemanticScholar(SemanticScholar):
    def __init__(
        self,
        req_rate_limit: float = 1,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._AsyncSemanticScholar = RateLimitedAsyncSemanticScholar(
            req_rate_limit=req_rate_limit,
            timeout=self._timeout,
            retry=self._retry,
            api_key=kwargs.get("api_key", None),
            api_url=kwargs.get("api_url", None),
            debug=kwargs.get("debug", False),
        )
