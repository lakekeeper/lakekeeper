"""Shared helpers for the agentic-medallion example.

Everything here runs INSIDE the docker network (the `workbench` JupyterLab
kernel), so the URLs default to in-network hostnames. Override via env if you
run elsewhere.

Two kinds of principal appear in this example:

* The **human operator** (peter) logs in interactively with the OAuth2
  device-authorization grant — `device_login()` prints a URL you approve in a
  browser. Peter is the platform admin: he bootstraps Lakekeeper, builds the
  warehouse + namespaces, and grants / revokes everyone else's access.
* Three **service accounts** authenticate non-interactively with the
  client-credentials grant (`get_token()`) — the right model for autonomous,
  no-human-in-the-loop jobs:
    - `PIPELINE`  writes the medallion (Bronze/Silver/Gold). Peter grants it write.
    - `ANALYST`   an AI agent granted read on Gold only.
    - `CONTRACTOR` an AI agent granted read on Bronze/Silver — denied Gold.
"""
from __future__ import annotations

import os
import time
from urllib.parse import urlsplit

import jwt
import requests

LAKEKEEPER_URL = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181")

# Keycloak. The kernel reaches Keycloak over the in-network address
# (`keycloak:8080`); the device-login *verification* URL it prints must open in
# your HOST browser, so device_login() rewrites the in-network origin to the
# host-facing one below. `up.sh` sets KEYCLOAK_BROWSER_URL to the address your
# browser uses (localhost by default; a host IP/DNS on a remote Docker host).
KEYCLOAK_ISSUER = os.environ.get("KEYCLOAK_ISSUER", "http://keycloak:8080/realms/iceberg")
KEYCLOAK_BROWSER_URL = os.environ.get("KEYCLOAK_BROWSER_URL", "http://localhost:30080")
_iss = urlsplit(KEYCLOAK_ISSUER)
KEYCLOAK_INTERNAL_ORIGIN = f"{_iss.scheme}://{_iss.netloc}"  # e.g. http://keycloak:8080
KEYCLOAK_TOKEN_URL = os.environ.get(
    "KEYCLOAK_TOKEN_URL", f"{KEYCLOAK_ISSUER}/protocol/openid-connect/token"
)
KEYCLOAK_DEVICE_AUTH_URL = os.environ.get(
    "KEYCLOAK_DEVICE_AUTH_URL", f"{KEYCLOAK_ISSUER}/protocol/openid-connect/auth/device"
)

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333")
WAREHOUSE_NAME = os.environ.get("WAREHOUSE_NAME", "medallion")

# Local OSS models served by Ollama (see the `ml` compose profile).
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
VISION_MODEL = os.environ.get("VISION_MODEL", "moondream")
# Small by default (~1.6 GB — it only writes the prose answer; retrieval is CLIP and
# governance is authz). Bump to a bigger model (e.g. qwen2.5:7b) for nicer summaries.
CHAT_MODEL = os.environ.get("CHAT_MODEL", "gemma2:2b")

CATALOG_URL = f"{LAKEKEEPER_URL}/catalog"
MANAGEMENT_URL = f"{LAKEKEEPER_URL}/management"

# Medallion layers.
#   raw    - a governed generic-table location holding the raw image OBJECTS in S3
#            (<location>/<id>.jpg). Bronze links to them by s3:// URI. (see gt.py)
#   bronze - Iceberg: clean metadata + the image's s3:// link
#   silver - Iceberg: vision captions
#   gold   - Lance generic table: CLIP embeddings (the asset the agents want)
NS_RAW = "raw"
RAW_TABLE = "images"
NS_BRONZE = "bronze"
NS_SILVER = "silver"
NS_GOLD = "gold"
GOLD_TABLE = "image_embeddings"
# CLIP ViT-B/32 text+image embedding dimension.
EMB_DIM = 512

# The public client used for the interactive device-code login. It has no secret
# (public client), has the device-authorization grant enabled, and mints
# `aud=lakekeeper` via its default client scope — so Lakekeeper accepts the token.
PUBLIC_CLIENT_ID = os.environ.get("PUBLIC_CLIENT_ID", "lakekeeper")

# Service-account identities (client_id, client_secret). These secrets live in
# keycloak/realm.json — demo only, never reuse. PIPELINE reuses the realm's
# pre-provisioned `bootstrap` client as a plain service account; it has NO
# special powers here — peter grants it write access in the setup notebook.
PIPELINE = ("bootstrap", "bootstrap-secret-0000000000000000")
ANALYST = ("analyst-agent", "analyst-secret-00000000000000000")
CONTRACTOR = ("contractor-agent", "contractor-secret-0000000000000000")

OK = (200, 201, 204)


def auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def subject(token: str) -> str:
    return jwt.decode(token, options={"verify_signature": False})["sub"]


# --- Service accounts: client-credentials grant (no human) -------------------

def get_token(client_id: str, client_secret: str) -> str:
    """Client-credentials grant against Keycloak, scoped for Lakekeeper."""
    r = requests.post(
        KEYCLOAK_TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "lakekeeper",
        },
        timeout=15,
    )
    r.raise_for_status()
    return r.json()["access_token"]


# --- Human operator: device-authorization grant (RFC 8628) -------------------

class DeviceSession:
    """A human login obtained via the device grant, refreshed transparently.

    Read `.token` wherever a bearer string is needed; it silently renews from the
    refresh token as it nears expiry, so the operator can pause between cells
    without their session going stale.
    """

    def __init__(self, tokens: dict):
        self._apply(tokens)
        self.username = jwt.decode(
            self._access, options={"verify_signature": False}
        ).get("preferred_username")

    def _apply(self, tokens: dict) -> None:
        self._access = tokens["access_token"]
        self._refresh = tokens.get("refresh_token")
        # Renew 30s early to avoid using a token that expires mid-request.
        self._expiry = time.time() + int(tokens.get("expires_in", 300)) - 30

    @property
    def token(self) -> str:
        if time.time() >= self._expiry and self._refresh:
            r = requests.post(
                KEYCLOAK_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "client_id": PUBLIC_CLIENT_ID,
                    "refresh_token": self._refresh,
                },
                timeout=15,
            )
            r.raise_for_status()
            self._apply(r.json())
        return self._access


def device_login(scope: str = "openid lakekeeper offline_access") -> DeviceSession:
    """Interactive human login via the OAuth2 device grant with PKCE.

    Prints a URL + code; approve it in your browser and this returns once you
    have. Uses the public `lakekeeper` client (no secret). `lakekeeper` grants
    the `aud=lakekeeper` the catalog requires; `offline_access` gets a refresh
    token so the session persists across a demo's pauses.
    """
    import base64
    import hashlib
    import secrets

    verifier = "".join(
        secrets.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        for _ in range(128)
    )
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
        .decode()
        .rstrip("=")
    )

    r = requests.post(
        KEYCLOAK_DEVICE_AUTH_URL,
        data={
            "client_id": PUBLIC_CLIENT_ID,
            "scope": scope,
            "code_challenge_method": "S256",
            "code_challenge": challenge,
        },
        timeout=15,
    )
    r.raise_for_status()
    d = r.json()

    # The URL comes back with the in-network origin (keycloak:8080); rewrite it
    # to the host-facing one so it opens in your browser. Token issuer is
    # unaffected — it stays keycloak:8080, Lakekeeper's primary (JWKS-reachable)
    # issuer, so no Keycloak reconfiguration is needed.
    url = d.get("verification_uri_complete") or d["verification_uri"]
    url = url.replace(KEYCLOAK_INTERNAL_ORIGIN, KEYCLOAK_BROWSER_URL)
    print("Open this URL in your browser and approve the login:\n")
    print(f"    {url}")
    print(f"\n(verification code: {d['user_code']})")
    _display_login_link(url, d["user_code"])

    interval = int(d.get("interval", 5))
    deadline = time.time() + int(d.get("expires_in", 600))
    while time.time() < deadline:
        time.sleep(interval)
        t = requests.post(
            KEYCLOAK_TOKEN_URL,
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": d["device_code"],
                "client_id": PUBLIC_CLIENT_ID,
                "code_verifier": verifier,
            },
            timeout=15,
        )
        if t.status_code == 200:
            session = DeviceSession(t.json())
            print(f"\n✓ logged in as {session.username}")
            return session
        err = t.json().get("error")
        if err == "slow_down":
            interval += 5
        elif err != "authorization_pending":
            raise RuntimeError(f"device login failed: {t.status_code} {t.text}")
    raise TimeoutError("device login timed out (the code expired before approval)")


def _display_login_link(url: str, code: str) -> None:
    """Render a clickable button in the notebook (best-effort; no-op in a plain shell)."""
    try:
        from IPython.display import HTML, display

        display(
            HTML(
                f'<div style="padding:12px;margin-top:8px;border:2px solid #4a90d9;'
                f'border-radius:8px;font-size:15px;max-width:640px">'
                f'👉 <a href="{url}" target="_blank"><b>Approve the login</b></a>'
                f'&nbsp;&nbsp;(verification code <code>{code}</code>)</div>'
            )
        )
    except Exception:  # noqa: BLE001 - display is a nicety, never fatal
        pass


# --- Catalog identity + warehouse helpers ------------------------------------

def whoami(token: str) -> dict:
    """Return the catalog user for a token. First touch provisions the user."""
    # Hitting the warehouse config endpoint is the documented provisioning
    # trigger; whoami then returns the freshly-created catalog user id.
    requests.get(
        f"{CATALOG_URL}/v1/config",
        params={"warehouse": WAREHOUSE_NAME},
        headers=auth(token),
        timeout=15,
    )
    r = requests.get(f"{MANAGEMENT_URL}/v1/whoami", headers=auth(token), timeout=15)
    r.raise_for_status()
    body = r.json()
    return body.get("user", body)


def agent_user_id(creds: tuple) -> str:
    """Provision (on first touch) and return the catalog user id for a service account."""
    return whoami(get_token(*creds))["id"]


def warehouse_id(token: str) -> str:
    """The warehouse UUID == the catalog URL prefix (from /catalog/v1/config)."""
    r = requests.get(
        f"{CATALOG_URL}/v1/config",
        params={"warehouse": WAREHOUSE_NAME},
        headers=auth(token),
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    prefix = data.get("overrides", {}).get("prefix") or data.get("defaults", {}).get("prefix")
    if not prefix:
        raise RuntimeError(f"no warehouse prefix in config response: {data}")
    return prefix


def namespace_id(token: str, name: str) -> str:
    """Resolve a namespace NAME to its UUID (required for permission grants).

    The catalog returns it in the namespace's properties under `namespace_id`.
    """
    wh = warehouse_id(token)
    r = requests.get(
        f"{CATALOG_URL}/v1/{wh}/namespaces/{name}", headers=auth(token), timeout=15
    )
    r.raise_for_status()
    props = r.json().get("properties", {})
    nsid = props.get("namespace_id")
    if not nsid:
        raise RuntimeError(f"no namespace_id in properties for '{name}': {props}")
    return nsid


# --- Permission grants (peter, the admin, is the only one who may call these) --

def _post_assignment(admin_token: str, url: str, user_id: str, relation: str, revoke: bool) -> int:
    assignment = {"type": relation, "user": user_id}
    body = {"deletes": [assignment]} if revoke else {"writes": [assignment]}
    r = requests.post(url, headers=auth(admin_token), json=body, timeout=15)
    if r.status_code not in OK:
        raise RuntimeError(f"assignment failed: {r.status_code} {r.text}")
    return r.status_code


def grant_warehouse(admin_token: str, wh_id: str, user_id: str, relation: str = "select",
                    revoke: bool = False) -> int:
    """Grant (or revoke) a warehouse-level relation for a user."""
    return _post_assignment(
        admin_token,
        f"{MANAGEMENT_URL}/v1/permissions/warehouse/{wh_id}/assignments",
        user_id, relation, revoke,
    )


def grant_namespace(admin_token: str, ns_id: str, user_id: str, relation: str = "select",
                    revoke: bool = False) -> int:
    """Grant (or revoke) a namespace-level relation for a user. This is the
    fine-grained lever the demo turns: read on some layers, denied on others."""
    return _post_assignment(
        admin_token,
        f"{MANAGEMENT_URL}/v1/permissions/namespace/{ns_id}/assignments",
        user_id, relation, revoke,
    )
