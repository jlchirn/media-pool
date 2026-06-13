import base64, hashlib, hmac, json, time


def _sign(payload: str, secret: str) -> str:
    raw = hmac.digest(secret.encode(), payload.encode(), "sha256")
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def create_event_token(secret: str, group_id: str = "default", ttl_days: int = 365) -> str:
    """Create an HMAC-signed token that encodes the group_id."""
    b64 = base64.urlsafe_b64encode(
        json.dumps({"exp": int(time.time()) + ttl_days * 86400, "grp": group_id}).encode()
    ).decode().rstrip("=")
    return f"{b64}.{_sign(b64, secret)}"


def verify_event_token(token: str, secret: str) -> str | None:
    """Verify signature and expiry.  Returns group_id on success, None on failure."""
    try:
        b64, sig = token.rsplit(".", 1)
        if not hmac.compare_digest(_sign(b64, secret), sig):
            return None
        data = json.loads(base64.urlsafe_b64decode(b64 + "=="))
        if data["exp"] <= int(time.time()):
            return None
        return data.get("grp", "default")
    except Exception:
        return None
