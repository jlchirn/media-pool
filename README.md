# Media Pool

Media Pool is a lightweight photo and video sharing app for private events.
Guests join by scanning a shared QR code, upload media without creating
accounts, and browse the shared gallery from a phone or desktop browser.
Event media is stored in the event owner's Dropbox account.

## Features

- QR-code based guest access
- No guest accounts required
- Photo and video uploads
- Dropbox-backed media storage
- Grid, list, timeline, and map views
- Reactions, comments, and view counts
- Mobile lightbox with swipe, pinch-to-zoom, and slideshow mode
- Live updates for uploads and social activity
- Optional AI captions and event summaries through Arena
- Optional highlight video generation with ffmpeg
- Multi-event QR admin page

## How It Works

The app runs as a FastAPI server. The owner configures Dropbox credentials and
an event folder. Guests scan a QR code that contains a signed event token.
Uploaded files are saved to Dropbox, while lightweight social metadata is stored
locally in SQLite.

## Requirements

- Python 3.10+
- Dropbox app credentials
- A Dropbox folder for event media
- Optional: ffmpeg for highlight video generation
- Optional: Arena server for AI captions and summaries

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Copy `config.example.env` to a local `.env` file and fill in your own values.
Do not commit `.env`.

```env
DROPBOX_APP_KEY=your_app_key_here
DROPBOX_APP_SECRET=your_app_secret_here
DROPBOX_REFRESH_TOKEN=your_refresh_token_here
SECRET_KEY=replace_with_a_long_random_string
ADMIN_PIN=change_me
PORT=7000
```

For multi-event mode, configure a Dropbox root folder:

```env
DROPBOX_GROUPS_ROOT=/MediaPool
```

Each event folder should contain an `event.json` file.

## Example event.json

```json
{
  "name": "Summer Trip 2026",
  "date": "2026-07-03",
  "valid_from": "2026-07-03T00:00:00",
  "valid_until": "2026-07-14T23:59:00",
  "theme": "default",
  "allow_video": true,
  "session_idle_hours": 24,
  "language": "English"
}
```

## Running Locally

```bash
python run.py
```

Open the admin QR page shown in the terminal. For phone access, open the LAN
address printed by the server from a device on the same Wi-Fi network.

## Running On Azure VM

Start the server with:

```bash
python -m run
```

If the VM has a public IPv4 address, the launcher prints an `Azure public URL`.
Open that URL from the admin browser so generated QR codes use the same reachable
host. For a stable deployment, set `PUBLIC_URL` in `.env`:

```env
PUBLIC_URL=http://your-public-ip-or-domain:7000
```

Use `https://` only after configuring a reverse proxy or certificate. Also allow
inbound TCP traffic for the app port in both the Azure network security group
and Windows Firewall.

## Project Structure

```text
client/            Frontend HTML, Alpine.js, and Tailwind UI
server/            FastAPI backend, Dropbox integration, and metadata storage
run.py             Local server launcher
requirements.txt   Python dependencies
```

## Security Notes

- Do not commit `.env`, real Dropbox credentials, local databases, logs, or
  uploaded media.
- Use a strong `SECRET_KEY` and `ADMIN_PIN`.
- QR links grant event access to anyone who has the link.
- Keep the app behind a trusted network or deployment boundary unless you add
  production-grade hosting controls.

## Limitations

- Guest identity is anonymous and session-based.
- Media storage depends on Dropbox API availability and configured permissions.
- AI features require a compatible Arena server.
- Local SQLite social metadata is not shared across multiple server instances.

## License

This project is licensed under the MIT License. See `LICENSE` for details.
