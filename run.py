"""Start the Media Pool server. Set PORT in .env to change the port (default 7000)."""
import asyncio
import json
import logging
import os
import socket
import urllib.request

import uvicorn
from dotenv import load_dotenv

load_dotenv()

VM_PREFIXES = ("192.168.122.", "192.168.124.", "192.168.136.", "192.168.56.", "192.168.99.")
AZURE_METADATA_URL = (
    "http://169.254.169.254/metadata/instance/network/interface"
    "?api-version=2021-02-01"
)


class _DropInvalidHttpWarning(logging.Filter):
    """Hide noisy malformed-request warnings from internet-facing HTTP ports."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "Invalid HTTP request received" not in record.getMessage()


def _configure_windows_event_loop():
    """Avoid noisy Proactor accept errors on Windows when clients disconnect early."""
    if os.name != "nt":
        return
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass


def _quiet_expected_public_http_noise():
    """Suppress common public-port noise unless explicitly requested."""
    show_invalid = os.getenv("SHOW_INVALID_HTTP_WARNINGS", "").strip().lower()
    if show_invalid in {"1", "true", "yes", "on"}:
        return
    logging.getLogger("uvicorn.error").addFilter(_DropInvalidHttpWarning())

def _get_lan_ips():
    """Return all non-loopback IPv4 addresses bound to this hostname."""
    ips = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127.") and not ip.startswith("169.254.") and ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    return ips


def _get_azure_public_ips():
    """Return public IPv4 addresses from Azure IMDS when running on an Azure VM."""
    req = urllib.request.Request(AZURE_METADATA_URL, headers={"Metadata": "true"})
    ips = []
    try:
        with urllib.request.urlopen(req, timeout=0.4) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return ips

    for iface in data.get("interface", []):
        for ip_config in iface.get("ipv4", {}).get("ipAddress", []):
            public_ips = ip_config.get("publicIpAddress", [])
            if isinstance(public_ips, dict):
                public_ips = [public_ips]
            for public_ip in public_ips:
                if isinstance(public_ip, dict):
                    ip = public_ip.get("ipAddress")
                else:
                    ip = str(public_ip)
                if ip and ip not in ips:
                    ips.append(ip)
    return ips


if __name__ == "__main__":
    _configure_windows_event_loop()
    _quiet_expected_public_http_noise()

    port = int(os.getenv("PORT", 7000))

    lan_ips = _get_lan_ips()
    public_ips = _get_azure_public_ips()
    good_ips = [ip for ip in lan_ips if not any(ip.startswith(v) for v in VM_PREFIXES)]
    vm_ips   = [ip for ip in lan_ips if any(ip.startswith(v) for v in VM_PREFIXES)]

    print()
    print("=" * 54)
    print("  Media Pool")
    print("=" * 54)
    if public_ips:
        for ip in public_ips:
            print(f"  Azure public URL : http://{ip}:{port}/qrs")
        print()
        print("  For phones outside the Azure private network, use")
        print("  the public URL above or set PUBLIC_URL in .env.")
        print("  Open it with http:// unless you configured HTTPS.")
        print()
    if good_ips:
        for ip in good_ips:
            print(f"  Admin / QR page : http://{ip}:{port}/qrs")
        print()
        print("  Open the admin page at ONE of the URLs above")
        print("  (use the IP your phone's Wi-Fi can reach).")
        print("  QR codes will use whichever IP you open it with.")
    else:
        print(f"  Admin / QR page : http://localhost:{port}/qrs")
        print()
        print("  WARNING: no LAN IP detected. Phones may not be able")
        print("  to scan QR codes. Check your Wi-Fi connection.")
    if vm_ips:
        print()
        print(f"  Skipped VM adapters: {', '.join(vm_ips)}")
    print("=" * 54)
    print()

    uvicorn.run(
        "server.main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        access_log=False,
        log_level="warning",
    )
