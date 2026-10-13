import argparse
import json
import os
import secrets
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
MANAGEMENT_API_VERSION = "2022-03-01"
GMAIL_SCOPES = (
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
)


def log(message: str) -> None:
    print(message, flush=True)


def run_az(az_path: str, args: list[str]) -> str:
    if az_path == "az":
        az_path = resolve_az_path()
    try:
        return subprocess.check_output([az_path, *args], text=True, stderr=subprocess.STDOUT).strip()
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"az {' '.join(args)} failed:\n{exc.output}") from exc


def resolve_az_path() -> str:
    candidates = (
        r"C:\Program Files (x86)\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
        r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd",
    )
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return "az"


def json_request(method: str, url: str, *, token: str | None = None, body: dict | None = None) -> dict:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {details}") from exc
    return json.loads(payload) if payload else {}


def form_request(url: str, fields: dict[str, str]) -> dict:
    data = urllib.parse.urlencode(fields).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Google token exchange failed with HTTP {exc.code}: {details}") from exc
    return json.loads(payload)


def get_app_settings(subscription_id: str, resource_group: str, app_name: str, token: str) -> dict[str, str]:
    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}"
        f"/resourceGroups/{urllib.parse.quote(resource_group)}"
        f"/providers/Microsoft.Web/sites/{urllib.parse.quote(app_name)}"
        f"/config/appsettings/list?api-version={MANAGEMENT_API_VERSION}"
    )
    response = json_request("POST", url, token=token)
    return response.get("properties") or {}


def put_app_settings(subscription_id: str, resource_group: str, app_name: str, token: str, settings: dict[str, str]) -> None:
    url = (
        f"https://management.azure.com/subscriptions/{subscription_id}"
        f"/resourceGroups/{urllib.parse.quote(resource_group)}"
        f"/providers/Microsoft.Web/sites/{urllib.parse.quote(app_name)}"
        f"/config/appsettings?api-version={MANAGEMENT_API_VERSION}"
    )
    json_request("PUT", url, token=token, body={"properties": settings})


def wait_for_callback(host: str, port: int, state: str, timeout_seconds: int) -> str:
    result: dict[str, str] = {}

    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            return

        def do_GET(self) -> None:
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            if (params.get("state") or [""])[0] != state:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"State mismatch. Return to Codex and retry the authorization.")
                return
            if "error" in params:
                result["error"] = (params.get("error_description") or params.get("error") or ["Unknown error"])[0]
            elif "code" in params:
                result["code"] = params["code"][0]
            else:
                result["error"] = "Callback did not include an authorization code."

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h1>Gmail authorization received</h1>"
                b"<p>You can close this tab and return to Codex.</p></body></html>"
            )

    server = ThreadingHTTPServer((host, port), CallbackHandler)
    server.timeout = 1
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline and "code" not in result and "error" not in result:
            server.handle_request()
    finally:
        server.server_close()

    if "error" in result:
        raise RuntimeError(f"Google authorization returned an error: {result['error']}")
    if "code" not in result:
        raise TimeoutError("Timed out waiting for Google authorization callback.")
    return result["code"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Reauthorize Gmail and store a new Azure Function refresh token.")
    parser.add_argument("--resource-group", default="aga-data-platform")
    parser.add_argument("--app-name", default="aga-clubexpress-mail")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument("--az-path", default="az")
    args = parser.parse_args()

    redirect_uri = f"http://{args.host}:{args.port}/"
    log(f"Fetching OAuth client settings for {args.app_name}...")
    subscription_id = run_az(args.az_path, ["account", "show", "--query", "id", "-o", "tsv"])
    management_token = run_az(args.az_path, ["account", "get-access-token", "--resource", "https://management.azure.com/", "--query", "accessToken", "-o", "tsv"])
    settings = get_app_settings(subscription_id, args.resource_group, args.app_name, management_token)

    client_id = settings.get("GOOGLE_WORKSPACE_CLIENT_ID")
    client_secret = settings.get("GOOGLE_WORKSPACE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Missing GOOGLE_WORKSPACE_CLIENT_ID or GOOGLE_WORKSPACE_CLIENT_SECRET in Azure app settings.")

    state = secrets.token_urlsafe(24)
    auth_params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(GMAIL_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "include_granted_scopes": "true",
        "state": state,
    }
    auth_url = f"{GOOGLE_AUTH_URL}?{urllib.parse.urlencode(auth_params)}"
    log("")
    log("Open this URL and authorize clubexpress@usgo.org:")
    log(auth_url)
    log("")
    log(f"Waiting up to {args.timeout_seconds} seconds for {redirect_uri} ...")

    code = wait_for_callback(args.host, args.port, state, args.timeout_seconds)
    token_response = form_request(
        GOOGLE_TOKEN_URL,
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": redirect_uri,
        },
    )
    refresh_token = token_response.get("refresh_token")
    if not refresh_token:
        scopes = token_response.get("scope", "")
        raise RuntimeError(
            "Google did not return a refresh token. Re-run and make sure the consent prompt is accepted. "
            f"Returned scopes: {scopes}"
        )

    returned_scopes = set(str(token_response.get("scope", "")).split())
    missing_scopes = set(GMAIL_SCOPES) - returned_scopes
    if missing_scopes:
        raise RuntimeError(f"Google token is missing required scopes: {', '.join(sorted(missing_scopes))}")

    settings["GOOGLE_WORKSPACE_REFRESH_TOKEN"] = refresh_token
    put_app_settings(subscription_id, args.resource_group, args.app_name, management_token, settings)
    log("SUCCESS: Stored a new GOOGLE_WORKSPACE_REFRESH_TOKEN in Azure.")
    log(f"Authorized scopes: {' '.join(sorted(returned_scopes))}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        log(f"ERROR: {exc}")
        raise SystemExit(1)
