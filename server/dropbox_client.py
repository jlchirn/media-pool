import mimetypes, os, threading
from datetime import datetime
from typing import Iterator, Optional
import dropbox
from dropbox.exceptions import ApiError
from dropbox import files as dbx_files
from dropbox.files import WriteMode

MIME_FALLBACK = "application/octet-stream"
STREAM_CHUNK = 65_536  # 64 KB chunks for streaming


def _is_not_found_error(exc: ApiError) -> bool:
    """Return True when Dropbox reports the target path is already missing."""
    err = getattr(exc, "error", None)
    try:
        if err and err.is_path_lookup():
            return err.get_path_lookup().is_not_found()
    except AttributeError:
        pass
    return "not_found" in str(exc).lower()


class DropboxClient:
    _shared_dbx = None
    _shared_key = None
    _shared_lock = threading.Lock()

    def __init__(self):
        key = (
            os.getenv("DROPBOX_APP_KEY"),
            os.getenv("DROPBOX_APP_SECRET"),
            os.getenv("DROPBOX_REFRESH_TOKEN"),
        )
        with self._shared_lock:
            if self.__class__._shared_dbx is None or self.__class__._shared_key != key:
                self.__class__._shared_dbx = dropbox.Dropbox(
                    app_key=key[0],
                    app_secret=key[1],
                    oauth2_refresh_token=key[2],
                )
                self.__class__._shared_key = key
        self._dbx = self.__class__._shared_dbx

    def download(self, path: str) -> tuple[bytes, str]:
        """Download entire file into memory. Use only for small files (thumbnails, event.json)."""
        _, res = self._dbx.files_download(path)
        mime = mimetypes.guess_type(path)[0] or MIME_FALLBACK
        return res.content, mime

    def download_stream(self, path: str) -> tuple[Iterator[bytes], str]:
        """Stream file from Dropbox in chunks — never buffers the whole file in RAM."""
        _, res = self._dbx.files_download(path)
        mime = mimetypes.guess_type(path)[0] or MIME_FALLBACK
        return res.iter_content(chunk_size=STREAM_CHUNK), mime

    def download_text(self, path: str) -> str:
        content, _ = self.download(path)
        return content.decode("utf-8")

    def upload(self, path: str, content: bytes,
               client_modified: Optional[datetime] = None) -> None:
        self._dbx.files_upload(
            content, path, mode=WriteMode.add, autorename=True,
            client_modified=client_modified,
        )

    def list_folder(self, path: str) -> list[dict]:
        result = self._dbx.files_list_folder(path)
        files = []
        while True:
            for entry in result.entries:
                if isinstance(entry, dbx_files.FileMetadata):
                    # client_modified = original file creation time on the device;
                    # fall back to server_modified if absent (rare edge case)
                    ts = entry.client_modified or entry.server_modified
                    files.append({
                        "name": entry.name,
                        "size": entry.size,
                        "modified": ts.isoformat(),
                    })
            if not result.has_more:
                break
            result = self._dbx.files_list_folder_continue(result.cursor)
        return sorted(files, key=lambda f: f["modified"], reverse=True)

    def get_temporary_link(self, path: str) -> str:
        """Return a 4-hour temporary direct-access URL for a file (supports Range requests)."""
        return self._dbx.files_get_temporary_link(path).link

    def delete(self, path: str, missing_ok: bool = False) -> bool:
        """Permanently delete a file from Dropbox. Returns False if already missing."""
        try:
            self._dbx.files_delete_v2(path)
            return True
        except ApiError as exc:
            if missing_ok and _is_not_found_error(exc):
                return False
            raise

    def get_thumbnail(self, path: str) -> bytes:
        _, res = self._dbx.files_get_thumbnail_v2(
            resource=dbx_files.PathOrLink.path(path),
            format=dbx_files.ThumbnailFormat.jpeg,
            size=dbx_files.ThumbnailSize.w640h480,
        )
        return res.content
