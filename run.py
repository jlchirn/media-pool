"""Start the Media Pool server. Set PORT in .env to change the port (default 7000)."""
import os
import socket
import uvicorn
from dotenv import load_dotenv

load_dotenv()

VM_PREFIXES = ("192.168.122.", "192.168.124.", "192.168.136.", "192.168.56.", "192.168.99.")

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

if __name__ == "__main__":
    port = int(os.getenv("PORT", 7000))

    lan_ips = _get_lan_ips()
    good_ips = [ip for ip in lan_ips if not any(ip.startswith(v) for v in VM_PREFIXES)]
    vm_ips   = [ip for ip in lan_ips if any(ip.startswith(v) for v in VM_PREFIXES)]

    print()
    print("=" * 54)
    print("  Media Pool")
    print("=" * 54)
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
