from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
import json

@dataclass(frozen=True)
class DownloadResult:
    success: bool
    message: str
    code: str
    status_code: Optional[int] = None
    bytes_written: int = 0
    via: str = "direct"


class DirectOrCloudflareStrategy:
    def __init__(
        self,
        headers: dict,
        timeout: int = 30,
        api_url: str = "http://108.181.6.19:8191/v1",
        api_max_timeout: int = 6000,
    ) -> None:
        self._headers = headers
        self._timeout = timeout
        self._api_url = api_url
        self._api_max_timeout = api_max_timeout
        self._session_cache: dict[str, requests.Session] = {}
        self._api_attempted_domains: set[str] = set()

    def download(self, url: str, dest: Path) -> DownloadResult:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.exists():
            return DownloadResult(
                success=True,
                message="Already exists",
                code="exists",
                via="direct",
            )

        domain = self._domain_from_url(url)
        cached_session = self._session_cache.get(domain)
        if cached_session is not None:
            cached = self._download_requests_once(
                url,
                dest,
                session=cached_session,
                via="cached",
            )
            if cached.success:
                return cached
            if not self._should_retry_with_api(cached):
                return cached
            if domain in self._api_attempted_domains:
                return cached

        direct = self._download_requests_once(url, dest, session=None, via="direct")
        if direct.success:
            return direct

        if self._should_retry_with_api(direct):
            if domain in self._api_attempted_domains:
                return direct
            self._api_attempted_domains.add(domain)
            return self._download_via_api(url, dest, domain=domain)

        return direct

    def _download_requests_once(
        self,
        url: str,
        dest: Path,
        session: Optional[requests.Session],
        via: str,
    ) -> DownloadResult:
        tmp_path = dest.with_suffix(dest.suffix + ".part")
        sess = session or requests.Session()
        try:
            response = sess.get(
                url,
                headers=self._headers,
                timeout=self._timeout,
                stream=True,
            )
        except requests.exceptions.RequestException as exc:
            return DownloadResult(
                success=False,
                message=f"Failed: {exc}",
                code="request_error",
                status_code=None,
                via=via,
            )

        status_code = response.status_code
        iterator = response.iter_content(chunk_size=8192)
        first_chunk = next(iterator, b"")

        if status_code >= 400:
            code = (
                "cloudflare_challenge"
                if self._is_cloudflare_challenge(response, first_chunk)
                else "http_error"
            )
            response.close()
            return DownloadResult(
                success=False,
                message=f"HTTP {status_code}",
                code=code,
                status_code=status_code,
                via=via,
            )

        if not self._is_pdf_response(response, first_chunk):
            code = (
                "cloudflare_challenge"
                if self._is_cloudflare_challenge(response, first_chunk)
                else "not_pdf"
            )
            response.close()
            return DownloadResult(
                success=False,
                message="Blocked by Cloudflare challenge"
                if code == "cloudflare_challenge"
                else "Not a PDF response",
                code=code,
                status_code=status_code,
                via=via,
            )

        bytes_written = 0
        try:
            with open(tmp_path, "wb") as handle:
                if first_chunk:
                    handle.write(first_chunk)
                    bytes_written += len(first_chunk)
                for chunk in iterator:
                    if not chunk:
                        continue
                    handle.write(chunk)
                    bytes_written += len(chunk)
            tmp_path.replace(dest)
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(
                success=False,
                message=f"Failed to write file: {exc}",
                code="write_error",
                via=via,
            )

        reported_size = response.headers.get("content-length")
        if reported_size is None:
            message = f"Downloaded ({bytes_written} bytes)"
        else:
            message = f"Downloaded ({reported_size} bytes)"

        return DownloadResult(
            success=True,
            message=message,
            code="ok",
            status_code=status_code,
            bytes_written=bytes_written,
            via=via,
        )

    def _download_via_api(self, url: str, dest: Path, domain: str) -> DownloadResult:
        from loguru import logger
        logger.debug(f"Attempting download via API for domain: {domain}")
        try:
            response = requests.post(
                self._api_url,
                headers={"Content-Type": "application/json"},
                json={
                    "cmd": "request.get",
                    "url": url,
                    "maxTimeout": self._api_max_timeout,
                    "returnOnlyCookies": True,
                },
                timeout=self._timeout + 60,
            )
            response_json = response.json()
            if response_json['status'] != "ok":
                raise ValueError(f"API returned non-ok status: {response_json['status']}")
            print(response_json)
            """
            {'status': 'ok', 'message': 'Challenge not detected!', 'solution': {'url': 'https://www.biorxiv.org/content/biorxiv/early/2016/10/16/081125.full.pdf', 'status': 200, 'cookies': [{'domain': '.www.biorxiv.org', 'expiry': 1767685873, 'httpOnly': True, 'name': '__cf_bm', 'path': '/', 'sameSite': 'None', 'secure': True, 'value': 'lzkf.8a2Y2WwRbrN0lpXHcPfm2taUjdWv3G8y8hXUdU-1767684073-1.0.1.1-5_eIOgJQkR2AewerTzAMvWLr03CMRhhvWjn_IYb5BlmmlPUObWC_uLRFAdgvxysE2qmM8puIhuEh.a_TR0kE02rLFeORkuw8tkyAejLqMWM'}], 'userAgent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'}, 'startTimestamp': 1767684072515, 'endTimestamp': 1767684074211, 'version': '3.4.6'}
            """
            cookies_raw = response_json['solution']['cookies']
            cookies = {cookie['name']: cookie['value'] for cookie in cookies_raw}
            ua = response_json['solution']['userAgent']

            real_response = requests.post(
                url,
                headers={**self._headers, "User-Agent": ua},
                cookies=cookies,
                timeout=self._timeout,
                stream=True,
            )
            print(real_response.status_code)
            print(real_response.text)
            print(real_response.headers)

            response = real_response
        except requests.exceptions.RequestException as exc:
            return DownloadResult(
                success=False,
                message=f"API request failed: {exc}",
                code="api_error",
                status_code=None,
                via="api",
            )

        try:
            result = response.json()
        except ValueError as exc:
            return DownloadResult(
                success=False,
                message=f"API response parsing failed: {exc}",
                code="api_parse_error",
                status_code=response.status_code,
                via="api",
            )

        if "solution" not in result:
            return DownloadResult(
                success=False,
                message=f"API response missing solution: {result}",
                code="api_no_solution",
                status_code=response.status_code,
                via="api",
            )

        solution = result["solution"]
        status_code = solution.get("status")
        response_text = solution.get("response", "")

        if status_code and status_code >= 400:
            return DownloadResult(
                success=False,
                message=f"API returned HTTP {status_code}",
                code="api_http_error",
                status_code=status_code,
                via="api",
            )

        first_chunk = response_text.encode() if response_text else b""
        if not self._is_pdf_content(first_chunk):
            return DownloadResult(
                success=False,
                message="API response is not a PDF",
                code="api_not_pdf",
                status_code=status_code,
                via="api",
            )

        tmp_path = dest.with_suffix(dest.suffix + ".part")
        try:
            with open(tmp_path, "wb") as handle:
                handle.write(first_chunk)
            tmp_path.replace(dest)
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(
                success=False,
                message=f"Failed to write file: {exc}",
                code="write_error",
                via="api",
            )

        return DownloadResult(
            success=True,
            message=f"Downloaded via API ({len(first_chunk)} bytes)",
            code="ok",
            status_code=status_code,
            bytes_written=len(first_chunk),
            via="api",
        )

    @staticmethod
    def _domain_from_url(url: str) -> str:
        return urlparse(url).netloc

    def _should_retry_with_api(self, result: DownloadResult) -> bool:
        if result.code == "cloudflare_challenge":
            return True

        if result.status_code in {403, 429, 503}:
            return True

        return False

    @staticmethod
    def _is_pdf_content(content: bytes) -> bool:
        return content.lstrip().startswith(b"%PDF-")

    @staticmethod
    def _is_pdf_response(response: requests.Response, first_chunk: bytes) -> bool:
        content_type = response.headers.get("content-type", "").lower()
        if "pdf" in content_type:
            return True
        return first_chunk.lstrip().startswith(b"%PDF-")

    @staticmethod
    def _is_cloudflare_challenge(
        response: requests.Response, first_chunk: bytes
    ) -> bool:
        server = response.headers.get("server", "").lower()
        if "cloudflare" in server:
            return True

        if response.headers.get("cf-ray") or response.headers.get("cf-cache-status"):
            return True

        text = first_chunk.decode(errors="ignore").lower()
        if "cloudflare" in text:
            return True

        markers = [
            "attention required",
            "just a moment",
            "cf_chl",
            "cf-chl",
            "challenge-platform",
            "verify you are human",
        ]
        return any(marker in text for marker in markers)
