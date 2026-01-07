from __future__ import annotations

import base64
from dataclasses import dataclass
from pathlib import Path
import re
from typing import Optional
from urllib.parse import urljoin

import requests
from loguru import logger


@dataclass(frozen=True)
class DownloadResult:
    success: bool
    message: str
    code: str
    status_code: Optional[int] = None
    bytes_written: int = 0
    via: str = "api"


class ApiDownloadError(RuntimeError):
    pass


class DirectOrCloudflareStrategy:
    def __init__(
        self,
        headers: dict,
        timeout: int = 30,
        api_url: str = "http://localhost:8191/v1",
        api_max_timeout: int = 60000,
    ) -> None:
        self._headers = headers
        self._timeout = timeout
        self._api_url = api_url
        self._api_max_timeout = api_max_timeout

    def download(self, url: str, dest: Path) -> DownloadResult:
        dest.parent.mkdir(parents=True, exist_ok=True)
        html_dest = dest.with_suffix(".html")
        json_dest = dest.with_suffix(".json")
        md_dest = dest.with_suffix(".md")
        if dest.exists() or html_dest.exists() or json_dest.exists() or md_dest.exists():
            return DownloadResult(
                success=True,
                message="Already exists",
                code="exists",
                via="api",
            )

        try:
            # Step 1: ask FlareSolverr for cookies + UA that can access the URL.
            solution = self._fetch_solution(url, return_body=True)
        except ApiDownloadError as exc:
            logger.error("FlareSolverr failed for url={}, error={}", url, exc)
            return DownloadResult(
                success=False,
                message=str(exc),
                code="api_error",
                via="api",
            )

        # Step 2: if FlareSolverr returned the response body, write it directly.
        body_result = self._download_with_response_body(url, dest, solution)
        if body_result is not None and (
            body_result.success or body_result.code not in {"not_pdf", "not_html"}
        ):
            return body_result

        # Step 3: if HTML is available in the response, store it as article content.
        html_result = self._download_with_html_response(url, dest, solution)
        if html_result is not None:
            return html_result

        # Step 4: attempt to discover a PDF link from the HTML response.
        pdf_url = self._extract_pdf_url(url, solution)
        if pdf_url:
            try:
                pdf_solution = self._fetch_solution(pdf_url, return_body=True)
            except ApiDownloadError as exc:
                logger.error("FlareSolverr failed for pdf url={}, error={}", pdf_url, exc)
                return DownloadResult(
                    success=False,
                    message=str(exc),
                    code="api_error",
                    via="api",
                )
            pdf_body_result = self._download_with_response_body(pdf_url, dest, pdf_solution)
            if pdf_body_result is not None and (
                pdf_body_result.success or pdf_body_result.code not in {"not_pdf", "not_html"}
            ):
                return pdf_body_result
            return self._download_with_solution(pdf_url, dest, pdf_solution)

        # Step 5: perform the real download using the provided cookies/UA.
        return self._download_with_solution(url, dest, solution)

    def _fetch_solution(self, url: str, return_body: bool = False) -> dict:
        # Only request cookies/UA; content is fetched by us afterwards.
        logger.debug(f"Requesting FlareSolverr solution for: {url}")
        payload = {
            "cmd": "request.get",
            "url": url,
            "maxTimeout": self._api_max_timeout,
        }
        if return_body:
            payload["returnResponseBody"] = True
        else:
            payload["returnOnlyCookies"] = True

        try:
            response = requests.post(
                self._api_url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=self._timeout + 30,
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.error("FlareSolverr API request error: url={}, error={}", url, exc)
            raise ApiDownloadError(f"API request failed: {exc}") from exc

        try:
            data = response.json()
        except ValueError as exc:
            logger.error("FlareSolverr API returned non-JSON: url={}, error={}", url, exc)
            raise ApiDownloadError(f"API response parsing failed: {exc}") from exc

        if data.get("status") != "ok":
            message = data.get("message") or data.get("status") or "unknown error"
            logger.error(
                "FlareSolverr status not ok: url={}, status={}, message={}",
                url,
                data.get("status"),
                message,
            )
            raise ApiDownloadError(f"API returned error: {message}")

        solution = data.get("solution")
        if not solution:
            logger.error("FlareSolverr response missing solution: url={}", url)
            raise ApiDownloadError("API response missing solution")

        logger.debug(
            "FlareSolverr solved: url={}, message={}, solution_status={}",
            url,
            data.get("message"),
            solution.get("status"),
        )
        return solution

    def _download_with_response_body(
        self,
        url: str,
        dest: Path,
        solution: dict,
    ) -> Optional[DownloadResult]:
        response_body = solution.get("responseBody")
        if not response_body:
            return None

        base64_encoded = bool(solution.get("responseBodyBase64"))
        try:
            if base64_encoded:
                payload = base64.b64decode(response_body)
            else:
                payload = response_body.encode("utf-8")
        except Exception as exc:
            logger.warning("Failed to decode response body: url={}, error={}", url, exc)
            return DownloadResult(
                success=False,
                message=f"Response body decode failed: {exc}",
                code="decode_error",
                via="api",
            )

        target_url = (solution.get("responseBodyUrl") or solution.get("url") or url).lower()
        mime_type = (solution.get("responseBodyMimeType") or "").lower()
        is_pdf_url = ".pdf" in target_url
        if payload.lstrip().startswith(b"%PDF-"):
            return self._write_payload(dest, payload, solution, code="ok")

        if is_pdf_url:
            logger.warning(
                "Non-PDF response body after solve: url={}, final_url={}, content_type={}",
                url,
                solution.get("responseBodyUrl") or solution.get("url") or url,
                solution.get("responseBodyMimeType"),
            )
            return DownloadResult(
                success=False,
                message="Not a PDF response",
                code="not_pdf",
                status_code=solution.get("responseBodyStatus"),
                via="api",
            )

        if "html" in mime_type:
            html_path = dest.with_suffix(".html")
            result = self._write_payload(html_path, payload, solution, code="html")
            if result.success:
                self._write_markdown(html_path)
            return result
        if "json" in mime_type:
            return self._write_payload(dest.with_suffix(".json"), payload, solution, code="json")

        return DownloadResult(
            success=False,
            message="Not a PDF response",
            code="not_html",
            status_code=solution.get("responseBodyStatus"),
            via="api",
        )

    def _download_with_html_response(
        self,
        url: str,
        dest: Path,
        solution: dict,
    ) -> Optional[DownloadResult]:
        html = solution.get("response")
        if not html or "<html" not in html.lower():
            return None

        payload = html.encode("utf-8")
        html_path = dest.with_suffix(".html")
        result = self._write_payload(html_path, payload, solution, code="html")
        if result.success:
            self._write_markdown(html_path)
        return result

    def _write_payload(
        self,
        dest: Path,
        payload: bytes,
        solution: dict,
        code: str,
    ) -> DownloadResult:
        tmp_path = dest.with_suffix(dest.suffix + ".part")
        try:
            with open(tmp_path, "wb") as handle:
                handle.write(payload)
            tmp_path.replace(dest)
        except OSError as exc:
            tmp_path.unlink(missing_ok=True)
            return DownloadResult(
                success=False,
                message=f"Failed to write file: {exc}",
                code="write_error",
                via="api",
            )

        message = f"Downloaded ({len(payload)} bytes)"
        return DownloadResult(
            success=True,
            message=message,
            code=code,
            status_code=solution.get("responseBodyStatus"),
            bytes_written=len(payload),
            via="api",
        )

    @staticmethod
    def _write_markdown(html_path: Path) -> None:
        try:
            from markitdown import MarkItDown
        except Exception as exc:
            logger.warning("markitdown unavailable, skip markdown conversion: {}", exc)
            return

        try:
            result = MarkItDown().convert(html_path)
        except Exception as exc:
            logger.warning("Failed to convert html to markdown: path={}, error={}", html_path, exc)
            return

        md_path = html_path.with_suffix(".md")
        try:
            with open(md_path, "w", encoding="utf-8") as handle:
                handle.write(result.text_content or "")
        except OSError as exc:
            logger.warning("Failed to write markdown: path={}, error={}", md_path, exc)

    @staticmethod
    def _extract_pdf_url(url: str, solution: dict) -> Optional[str]:
        html = solution.get("response") or ""
        if not html:
            return None

        patterns = [
            r'href=["\\\']([^"\\\']+?\\.pdf(?:\\?[^"\\\']*)?)["\\\']',
            r'href=["\\\']([^"\\\']+/pdf/[^"\\\']*)["\\\']',
        ]
        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                return urljoin(solution.get("url") or url, match.group(1))

        return None

    def _download_with_solution(self, url: str, dest: Path, solution: dict) -> DownloadResult:
        # Build request context from FlareSolverr solution.
        cookies_raw = solution.get("cookies") or []
        cookies = {
            cookie["name"]: cookie["value"]
            for cookie in cookies_raw
            if cookie.get("name") and cookie.get("value")
        }
        user_agent = solution.get("userAgent") or self._headers.get("User-Agent")
        # FlareSolverr may return a canonical URL after redirects.
        target_url = solution.get("url") or url

        if not cookies:
            logger.warning("FlareSolverr returned no cookies: url={}", url)
        if not solution.get("userAgent"):
            logger.warning("FlareSolverr returned no userAgent: url={}", url)
        if solution.get("status") and solution.get("status") >= 400:
            logger.warning(
                "FlareSolverr solution status indicates failure: url={}, status={}",
                url,
                solution.get("status"),
            )

        session = requests.Session()
        session.headers.update(self._headers)
        if user_agent:
            session.headers["User-Agent"] = user_agent

        try:
            # Follow redirects to the final PDF endpoint.
            response = session.get(
                target_url,
                cookies=cookies,
                timeout=self._timeout,
                stream=True,
                allow_redirects=True,
            )
        except requests.exceptions.RequestException as exc:
            logger.error(
                "Download request failed after solve: url={}, target_url={}, error={}",
                url,
                target_url,
                exc,
            )
            return DownloadResult(
                success=False,
                message=f"Request failed: {exc}",
                code="request_error",
                status_code=None,
                via="api",
            )

        status_code = response.status_code
        if response.history:
            logger.debug(
                "Redirected {} times: url={}, final_url={}",
                len(response.history),
                url,
                response.url,
            )
        iterator = response.iter_content(chunk_size=8192)
        first_chunk = next(iterator, b"")

        if status_code >= 400:
            response.close()
            logger.warning(
                "HTTP error after solve: url={}, final_url={}, status={}",
                url,
                response.url,
                status_code,
            )
            return DownloadResult(
                success=False,
                message=f"HTTP {status_code}",
                code="http_error",
                status_code=status_code,
                via="api",
            )

        # Fast check to avoid writing non-PDF responses.
        if not self._is_pdf_response(response, first_chunk):
            response.close()
            logger.warning(
                "Non-PDF response after solve: url={}, final_url={}, content_type={}",
                url,
                response.url,
                response.headers.get("content-type"),
            )
            return DownloadResult(
                success=False,
                message="Not a PDF response",
                code="not_pdf",
                status_code=status_code,
                via="api",
            )

        tmp_path = dest.with_suffix(dest.suffix + ".part")
        bytes_written = 0
        try:
            # Stream to disk to avoid loading large PDFs into memory.
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
                via="api",
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
            via="api",
        )

    @staticmethod
    def _is_pdf_response(response: requests.Response, first_chunk: bytes) -> bool:
        content_type = response.headers.get("content-type", "").lower()
        if "pdf" in content_type:
            return True
        return first_chunk.lstrip().startswith(b"%PDF-")
