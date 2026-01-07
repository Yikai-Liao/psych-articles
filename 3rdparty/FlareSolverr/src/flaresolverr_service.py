import json
import logging
import os
import platform
import sys
import time
from datetime import timedelta
from html import escape
from urllib.parse import unquote, quote

from func_timeout import FunctionTimedOut, func_timeout
from selenium.common import TimeoutException
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.expected_conditions import (
    presence_of_element_located, staleness_of, title_is)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.wait import WebDriverWait

import utils
from dtos import (STATUS_ERROR, STATUS_OK, ChallengeResolutionResultT,
                  ChallengeResolutionT, HealthResponse, IndexResponse,
                  V1RequestBase, V1ResponseBase)
from sessions import SessionsStorage

ACCESS_DENIED_TITLES = [
    # Cloudflare
    'Access denied',
    # Cloudflare http://bitturk.net/ Firefox
    'Attention Required! | Cloudflare'
]
ACCESS_DENIED_SELECTORS = [
    # Cloudflare
    'div.cf-error-title span.cf-code-label span',
    # Cloudflare http://bitturk.net/ Firefox
    '#cf-error-details div.cf-error-overview h1'
]
CHALLENGE_TITLES = [
    # Cloudflare
    'Just a moment...',
    # DDoS-GUARD
    'DDoS-Guard'
]
CHALLENGE_SELECTORS = [
    # Cloudflare
    '#cf-challenge-running', '.ray_id', '.attack-box', '#cf-please-wait', '#challenge-spinner', '#trk_jschal_js', '#turnstile-wrapper', '.lds-ring',
    # Custom CloudFlare for EbookParadijs, Film-Paleis, MuziekFabriek and Puur-Hollands
    'td.info #js_info',
    # Fairlane / pararius.com
    'div.vc div.text-box h2'
]

TURNSTILE_SELECTORS = [
    "input[name='cf-turnstile-response']"
]

SHORT_TIMEOUT = 1
SESSIONS_STORAGE = SessionsStorage()
_REDACT_LOG_KEYS = {"response", "responseBody", "screenshot"}


def _get_open_fd_count() -> int | None:
    if os.name != "posix":
        return None
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return None


def _redact_large_fields(payload):
    if not isinstance(payload, dict):
        return payload
    redacted = {}
    for key, value in payload.items():
        if key in _REDACT_LOG_KEYS and value is not None:
            if isinstance(value, str):
                redacted[key] = f"<omitted len={len(value)}>"
            else:
                redacted[key] = "<omitted>"
            continue
        if isinstance(value, dict):
            redacted[key] = _redact_large_fields(value)
        elif isinstance(value, list):
            redacted[key] = [
                _redact_large_fields(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            redacted[key] = value
    return redacted


def test_browser_installation():
    logging.info("Testing web browser installation...")
    logging.info("Platform: " + platform.platform())

    chrome_exe_path = utils.get_chrome_exe_path()
    if chrome_exe_path is None:
        logging.error("Chrome / Chromium web browser not installed!")
        sys.exit(1)
    else:
        logging.info("Chrome / Chromium path: " + chrome_exe_path)

    chrome_major_version = utils.get_chrome_major_version()
    if chrome_major_version == '':
        logging.error("Chrome / Chromium version not detected!")
        sys.exit(1)
    else:
        logging.info("Chrome / Chromium major version: " + chrome_major_version)

    logging.info("Launching web browser...")
    user_agent = utils.get_user_agent()
    logging.info("FlareSolverr User-Agent: " + user_agent)
    logging.info("Test successful!")


def index_endpoint() -> IndexResponse:
    res = IndexResponse({})
    res.msg = "FlareSolverr is ready!"
    res.version = utils.get_flaresolverr_version()
    res.userAgent = utils.get_user_agent()
    return res


def health_endpoint() -> HealthResponse:
    res = HealthResponse({})
    res.status = STATUS_OK
    return res


def controller_v1_endpoint(req: V1RequestBase) -> V1ResponseBase:
    start_ts = int(time.time() * 1000)
    req_payload = _redact_large_fields(utils.object_to_dict(req))
    open_fds = _get_open_fd_count()
    if open_fds is None:
        logging.info("Incoming request => POST /v1 body: %s", req_payload)
    else:
        logging.info("Incoming request => POST /v1 body: %s | open_fds=%s", req_payload, open_fds)
    res: V1ResponseBase
    try:
        res = _controller_v1_handler(req)
    except Exception as e:
        res = V1ResponseBase({})
        res.__error_500__ = True
        res.status = STATUS_ERROR
        res.message = "Error: " + str(e)
        logging.error(res.message)

    res.startTimestamp = start_ts
    res.endTimestamp = int(time.time() * 1000)
    res.version = utils.get_flaresolverr_version()
    logging.debug("Response => POST /v1 body: %s", _redact_large_fields(utils.object_to_dict(res)))
    open_fds = _get_open_fd_count()
    if open_fds is None:
        logging.info("Response in %.3f s", (res.endTimestamp - res.startTimestamp) / 1000)
    else:
        logging.info("Response in %.3f s | open_fds=%s", (res.endTimestamp - res.startTimestamp) / 1000, open_fds)
    return res


def _controller_v1_handler(req: V1RequestBase) -> V1ResponseBase:
    # do some validations
    if req.cmd is None:
        raise Exception("Request parameter 'cmd' is mandatory.")
    if req.headers is not None:
        logging.warning("Request parameter 'headers' was removed in FlareSolverr v2.")
    if req.userAgent is not None:
        logging.warning("Request parameter 'userAgent' was removed in FlareSolverr v2.")

    # set default values
    if req.maxTimeout is None or int(req.maxTimeout) < 1:
        req.maxTimeout = 60000

    # execute the command
    res: V1ResponseBase
    if req.cmd == 'sessions.create':
        res = _cmd_sessions_create(req)
    elif req.cmd == 'sessions.list':
        res = _cmd_sessions_list(req)
    elif req.cmd == 'sessions.destroy':
        res = _cmd_sessions_destroy(req)
    elif req.cmd == 'request.get':
        res = _cmd_request_get(req)
    elif req.cmd == 'request.post':
        res = _cmd_request_post(req)
    else:
        raise Exception(f"Request parameter 'cmd' = '{req.cmd}' is invalid.")

    return res


def _cmd_request_get(req: V1RequestBase) -> V1ResponseBase:
    # do some validations
    if req.url is None:
        raise Exception("Request parameter 'url' is mandatory in 'request.get' command.")
    if req.postData is not None:
        raise Exception("Cannot use 'postBody' when sending a GET request.")
    if req.returnRawHtml is not None:
        logging.warning("Request parameter 'returnRawHtml' was removed in FlareSolverr v2.")
    if req.download is not None:
        logging.warning("Request parameter 'download' was removed in FlareSolverr v2.")

    challenge_res = _resolve_challenge(req, 'GET')
    res = V1ResponseBase({})
    res.status = challenge_res.status
    res.message = challenge_res.message
    res.solution = challenge_res.result
    return res


def _cmd_request_post(req: V1RequestBase) -> V1ResponseBase:
    # do some validations
    if req.postData is None:
        raise Exception("Request parameter 'postData' is mandatory in 'request.post' command.")
    if req.returnRawHtml is not None:
        logging.warning("Request parameter 'returnRawHtml' was removed in FlareSolverr v2.")
    if req.download is not None:
        logging.warning("Request parameter 'download' was removed in FlareSolverr v2.")

    challenge_res = _resolve_challenge(req, 'POST')
    res = V1ResponseBase({})
    res.status = challenge_res.status
    res.message = challenge_res.message
    res.solution = challenge_res.result
    return res


def _cmd_sessions_create(req: V1RequestBase) -> V1ResponseBase:
    logging.debug("Creating new session...")

    session, fresh = SESSIONS_STORAGE.create(session_id=req.session, proxy=req.proxy)
    session_id = session.session_id

    if not fresh:
        return V1ResponseBase({
            "status": STATUS_OK,
            "message": "Session already exists.",
            "session": session_id
        })

    return V1ResponseBase({
        "status": STATUS_OK,
        "message": "Session created successfully.",
        "session": session_id
    })


def _cmd_sessions_list(req: V1RequestBase) -> V1ResponseBase:
    session_ids = SESSIONS_STORAGE.session_ids()

    return V1ResponseBase({
        "status": STATUS_OK,
        "message": "",
        "sessions": session_ids
    })


def _cmd_sessions_destroy(req: V1RequestBase) -> V1ResponseBase:
    session_id = req.session
    existed = SESSIONS_STORAGE.destroy(session_id)

    if not existed:
        raise Exception("The session doesn't exist.")

    return V1ResponseBase({
        "status": STATUS_OK,
        "message": "The session has been removed."
    })


def _resolve_challenge(req: V1RequestBase, method: str) -> ChallengeResolutionT:
    timeout = int(req.maxTimeout) / 1000
    driver = None
    try:
        if req.session:
            session_id = req.session
            ttl = timedelta(minutes=req.session_ttl_minutes) if req.session_ttl_minutes else None
            session, fresh = SESSIONS_STORAGE.get(session_id, ttl)

            if fresh:
                logging.debug(f"new session created to perform the request (session_id={session_id})")
            else:
                logging.debug(f"existing session is used to perform the request (session_id={session_id}, "
                              f"lifetime={str(session.lifetime())}, ttl={str(ttl)})")

            driver = session.driver
        else:
            enable_cdp_events = bool(req.returnResponseBody)
            driver = utils.get_webdriver(req.proxy, enable_cdp_events=enable_cdp_events)
            logging.debug('New instance of webdriver has been created to perform the request')
        return func_timeout(timeout, _evil_logic, (req, driver, method))
    except FunctionTimedOut:
        raise Exception(f'Error solving the challenge. Timeout after {timeout} seconds.')
    except Exception as e:
        raise Exception('Error solving the challenge. ' + str(e).replace('\n', '\\n'))
    finally:
        if not req.session and driver is not None:
            if utils.PLATFORM_VERSION == "nt":
                driver.close()
            driver.quit()
            logging.debug('A used instance of webdriver has been destroyed')


def click_verify(driver: WebDriver, num_tabs: int = 1):
    try:
        logging.debug("Try to find the Cloudflare verify checkbox...")
        actions = ActionChains(driver)
        actions.pause(5)
        for _ in range(num_tabs):
            actions.send_keys(Keys.TAB).pause(0.1)
        actions.pause(1)
        actions.send_keys(Keys.SPACE).perform()
        
        logging.debug(f"Cloudflare verify checkbox clicked after {num_tabs} tabs!")
    except Exception:
        logging.debug("Cloudflare verify checkbox not found on the page.")
    finally:
        driver.switch_to.default_content()

    try:
        logging.debug("Try to find the Cloudflare 'Verify you are human' button...")
        button = driver.find_element(
            by=By.XPATH,
            value="//input[@type='button' and @value='Verify you are human']",
        )
        if button:
            actions = ActionChains(driver)
            actions.move_to_element_with_offset(button, 5, 7)
            actions.click(button)
            actions.perform()
            logging.debug("The Cloudflare 'Verify you are human' button found and clicked!")
    except Exception:
        logging.debug("The Cloudflare 'Verify you are human' button not found on the page.")

    time.sleep(2)

def _get_turnstile_token(driver: WebDriver, tabs: int):
    token_input = driver.find_element(By.CSS_SELECTOR, "input[name='cf-turnstile-response']")
    current_value = token_input.get_attribute("value")
    while True:
        click_verify(driver, num_tabs=tabs)
        turnstile_token = token_input.get_attribute("value")
        if turnstile_token:
            if turnstile_token != current_value:
                logging.info(f"Turnstile token: {turnstile_token}")
                return turnstile_token
        logging.debug(f"Failed to extract token possibly click failed")        

        # reset focus
        driver.execute_script("""
            let el = document.createElement('button');
            el.style.position='fixed';
            el.style.top='0';
            el.style.left='0';
            document.body.prepend(el);
            el.focus();
        """)
        time.sleep(1)

def _resolve_turnstile_captcha(req: V1RequestBase, driver: WebDriver):
    turnstile_token = None
    if req.tabs_till_verify is not None:
        logging.debug(f'Navigating to... {req.url} in order to pass the turnstile challenge')
        driver.get(req.url)

        turnstile_challenge_found = False
        for selector in TURNSTILE_SELECTORS:
            found_elements = driver.find_elements(By.CSS_SELECTOR, selector)   
            if len(found_elements) > 0:
                turnstile_challenge_found = True
                logging.info("Turnstile challenge detected. Selector found: " + selector)
                break
        if turnstile_challenge_found:
            turnstile_token = _get_turnstile_token(driver=driver, tabs=req.tabs_till_verify)
        else:
            logging.debug(f'Turnstile challenge not found')
    return turnstile_token

def _normalize_url(url: str) -> str:
    return url.strip().lower().rstrip("/")


def _score_response_candidate(url: str, mime_type: str, status: int | None, resource_type: str | None, targets: set[str]) -> int:
    score = 0
    normalized_url = _normalize_url(url)
    if normalized_url in targets:
        score += 10
    if any(normalized_url.startswith(target) or target.startswith(normalized_url) for target in targets):
        score += 4
    if "pdf" in mime_type.lower():
        score += 6
    if ".pdf" in normalized_url:
        score += 4
    if resource_type == "Document":
        score += 2
    if status is not None and 200 <= int(status) < 300:
        score += 1
    return score


def _extract_response_body(req: V1RequestBase, driver: WebDriver) -> dict | None:
    try:
        perf_logs = driver.get_log("performance")
    except Exception as exc:
        logging.debug("Performance log unavailable: %s", exc)
        return None

    targets = {_normalize_url(url) for url in [req.url, driver.current_url] if url}
    candidates = []
    for index, entry in enumerate(perf_logs):
        message_text = entry.get("message")
        if not message_text:
            continue
        try:
            message = json.loads(message_text).get("message", {})
        except Exception:
            continue
        if message.get("method") != "Network.responseReceived":
            continue
        params = message.get("params", {})
        response = params.get("response", {})
        request_id = params.get("requestId")
        url = response.get("url")
        if not request_id or not url:
            continue
        mime_type = response.get("mimeType", "")
        status = response.get("status")
        resource_type = params.get("type")
        score = _score_response_candidate(url, mime_type, status, resource_type, targets)
        if score > 0:
            candidates.append((score, index, request_id, url, mime_type, status))

    if not candidates:
        logging.debug("No matching network response found for url=%s", req.url)
        return None

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    _, _, request_id, url, mime_type, status = candidates[0]

    try:
        body_result = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
    except Exception as exc:
        logging.debug("Network.getResponseBody failed: %s", exc)
        return None

    body = body_result.get("body")
    if body is None:
        return None

    return {
        "body": body,
        "base64Encoded": bool(body_result.get("base64Encoded")),
        "url": url,
        "mimeType": mime_type,
        "status": status,
    }


def _fetch_body_via_fetch(req: V1RequestBase, driver: WebDriver) -> dict | None:
    if req.postData is not None:
        return None
    script = """
    const url = arguments[0];
    const done = arguments[1];
    fetch(url, {credentials: 'include'}).then(async (resp) => {
      const buffer = await resp.arrayBuffer();
      const bytes = new Uint8Array(buffer);
      let binary = '';
      const chunk = 0x8000;
      for (let i = 0; i < bytes.length; i += chunk) {
        binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
      }
      const base64 = btoa(binary);
      done({
        ok: true,
        status: resp.status,
        contentType: resp.headers.get('content-type'),
        base64: base64
      });
    }).catch((err) => {
      done({ok: false, error: String(err)});
    });
    """
    try:
        result = driver.execute_async_script(script, req.url)
    except Exception as exc:
        logging.debug("Fetch response body failed: %s", exc)
        return None
    if not isinstance(result, dict) or not result.get("ok"):
        logging.debug("Fetch response body not ok: %s", result)
        return None
    body = result.get("base64")
    if not body:
        return None
    return {
        "body": body,
        "base64Encoded": True,
        "url": req.url,
        "mimeType": result.get("contentType") or "",
        "status": result.get("status"),
    }


def _evil_logic(req: V1RequestBase, driver: WebDriver, method: str) -> ChallengeResolutionT:
    res = ChallengeResolutionT({})
    res.status = STATUS_OK
    res.message = ""

    # optionally block resources like images/css/fonts using CDP
    disable_media = utils.get_config_disable_media()
    if req.disableMedia is not None:
        disable_media = req.disableMedia
    capture_response_body = bool(req.returnResponseBody)
    if disable_media or capture_response_body:
        block_urls = [
            # Images
            "*.png", "*.jpg", "*.jpeg", "*.gif", "*.webp", "*.bmp", "*.svg", "*.ico",
            "*.PNG", "*.JPG", "*.JPEG", "*.GIF", "*.WEBP", "*.BMP", "*.SVG", "*.ICO",
            "*.tiff", "*.tif", "*.jpe", "*.apng", "*.avif", "*.heic", "*.heif",
            "*.TIFF", "*.TIF", "*.JPE", "*.APNG", "*.AVIF", "*.HEIC", "*.HEIF",
            # Stylesheets
            "*.css",
            "*.CSS",
            # Fonts
            "*.woff", "*.woff2", "*.ttf", "*.otf", "*.eot",
            "*.WOFF", "*.WOFF2", "*.TTF", "*.OTF", "*.EOT"
        ]
        try:
            logging.debug("Network.setBlockedURLs: %s", block_urls)
            driver.execute_cdp_cmd("Network.enable", {})
            if disable_media:
                driver.execute_cdp_cmd("Network.setBlockedURLs", {"urls": block_urls})
        except Exception:
            # if CDP commands are not available or fail, ignore and continue
            logging.debug("Network CDP commands failed or unsupported on this webdriver")

    # navigate to the page
    logging.debug(f"Navigating to... {req.url}")
    turnstile_token = None

    if method == "POST":
        _post_request(req, driver)
    else:
        if req.tabs_till_verify is None:
            driver.get(req.url)
        else:
            turnstile_token = _resolve_turnstile_captcha(req, driver)

    # set cookies if required
    if req.cookies is not None and len(req.cookies) > 0:
        logging.debug(f'Setting cookies...')
        for cookie in req.cookies:
            driver.delete_cookie(cookie['name'])
            driver.add_cookie(cookie)
        # reload the page
        if method == 'POST':
            _post_request(req, driver)
        else:
            driver.get(req.url)

    # wait for the page
    if utils.get_config_log_html():
        logging.debug("Response HTML length: %s", len(driver.page_source))
    html_element = driver.find_element(By.TAG_NAME, "html")
    page_title = driver.title

    # find access denied titles
    for title in ACCESS_DENIED_TITLES:
        if page_title.startswith(title):
            raise Exception('Cloudflare has blocked this request. '
                            'Probably your IP is banned for this site, check in your web browser.')
    # find access denied selectors
    for selector in ACCESS_DENIED_SELECTORS:
        found_elements = driver.find_elements(By.CSS_SELECTOR, selector)
        if len(found_elements) > 0:
            raise Exception('Cloudflare has blocked this request. '
                            'Probably your IP is banned for this site, check in your web browser.')

    # find challenge by title
    challenge_found = False
    for title in CHALLENGE_TITLES:
        if title.lower() == page_title.lower():
            challenge_found = True
            logging.info("Challenge detected. Title found: " + page_title)
            break
    if not challenge_found:
        # find challenge by selectors
        for selector in CHALLENGE_SELECTORS:
            found_elements = driver.find_elements(By.CSS_SELECTOR, selector)
            if len(found_elements) > 0:
                challenge_found = True
                logging.info("Challenge detected. Selector found: " + selector)
                break

    attempt = 0
    if challenge_found:
        while True:
            try:
                attempt = attempt + 1
                # wait until the title changes
                for title in CHALLENGE_TITLES:
                    logging.debug("Waiting for title (attempt " + str(attempt) + "): " + title)
                    WebDriverWait(driver, SHORT_TIMEOUT).until_not(title_is(title))

                # then wait until all the selectors disappear
                for selector in CHALLENGE_SELECTORS:
                    logging.debug("Waiting for selector (attempt " + str(attempt) + "): " + selector)
                    WebDriverWait(driver, SHORT_TIMEOUT).until_not(
                        presence_of_element_located((By.CSS_SELECTOR, selector)))

                # all elements not found
                break

            except TimeoutException:
                logging.debug("Timeout waiting for selector")

                click_verify(driver)

                # update the html (cloudflare reloads the page every 5 s)
                html_element = driver.find_element(By.TAG_NAME, "html")

        # waits until cloudflare redirection ends
        logging.debug("Waiting for redirect")
        # noinspection PyBroadException
        try:
            WebDriverWait(driver, SHORT_TIMEOUT).until(staleness_of(html_element))
        except Exception:
            logging.debug("Timeout waiting for redirect")

        logging.info("Challenge solved!")
        res.message = "Challenge solved!"
    else:
        logging.info("Challenge not detected!")
        res.message = "Challenge not detected!"

    challenge_res = ChallengeResolutionResultT({})
    challenge_res.url = driver.current_url
    challenge_res.status = 200  # todo: fix, selenium not provides this info
    challenge_res.cookies = driver.get_cookies()
    challenge_res.userAgent = utils.get_user_agent(driver)
    challenge_res.turnstile_token = turnstile_token

    if not req.returnOnlyCookies:
        challenge_res.headers = {}  # todo: fix, selenium not provides this info

        if req.waitInSeconds and req.waitInSeconds > 0:
            logging.info("Waiting " + str(req.waitInSeconds) + " seconds before returning the response...")
            time.sleep(req.waitInSeconds)

        challenge_res.response = driver.page_source

    if capture_response_body:
        response_body = _extract_response_body(req, driver)
        if response_body is None:
            response_body = _fetch_body_via_fetch(req, driver)
        if response_body is None:
            logging.debug("Response body capture failed for url=%s", req.url)
        else:
            challenge_res.responseBody = response_body["body"]
            challenge_res.responseBodyBase64 = response_body["base64Encoded"]
            challenge_res.responseBodyUrl = response_body["url"]
            challenge_res.responseBodyMimeType = response_body["mimeType"]
            challenge_res.responseBodyStatus = response_body["status"]

    if req.returnScreenshot:
        challenge_res.screenshot = driver.get_screenshot_as_base64()

    res.result = challenge_res
    return res


def _post_request(req: V1RequestBase, driver: WebDriver):
    post_form = f'<form id="hackForm" action="{req.url}" method="POST">'
    query_string = req.postData if req.postData and req.postData[0] != '?' else req.postData[1:] if req.postData else ''
    pairs = query_string.split('&')
    for pair in pairs:
        parts = pair.split('=', 1)
        # noinspection PyBroadException
        try:
            name = unquote(parts[0])
        except Exception:
            name = parts[0]
        if name == 'submit':
            continue
        # noinspection PyBroadException
        try:
            value = unquote(parts[1]) if len(parts) > 1 else ''
        except Exception:
            value = parts[1] if len(parts) > 1 else ''
        # Protection of " character, for syntax
        value=value.replace('"','&quot;')
        post_form += f'<input type="text" name="{escape(quote(name))}" value="{escape(quote(value))}"><br>'
    post_form += '</form>'
    html_content = f"""
        <!DOCTYPE html>
        <html>
        <body>
            {post_form}
            <script>document.getElementById('hackForm').submit();</script>
        </body>
        </html>"""
    driver.get("data:text/html;charset=utf-8,{html_content}".format(html_content=html_content))
