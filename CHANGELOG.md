# Changelog

## 2026-06-14

- Changed media deletion to allow deleting any selected event item, with a clearer confirmation prompt that the deletion affects everyone.
- Made Dropbox deletion tolerant of files or thumbnails that are already missing, so cleanup can still remove local metadata.
- Cleared thumbnail and missing-thumbnail caches when media is deleted to avoid stale gallery state.
