# Next Steps

Where the project stands and what comes next, in priority order. Each item
lists the work, why it matters, and its acceptance criteria. Milestone tags
refer to the build guide.

## Phase A — Close out v1 (do first)

### A1. Run the M4 FUSE acceptance on a real mount (highest priority)

The FUSE layer compiles and fails gracefully without macFUSE, but has never
served a real mount.

-  On macOS: install macFUSE (`brew install --cask macfuse`), reboot, then:
  ```
  go build -o cloudstream ./cmd/cloudstream
  ./cloudstream mount ~/CloudDrive
  ls -lh ~/CloudDrive/movies/movie.mkv
  dd if=~/CloudDrive/movies/movie.mkv bs=1M skip=500 count=10 | md5
  ```
-  On Linux: `sudo apt install fuse3 libfuse3-dev` and the same steps.
-  **Acceptance:** `ls`/`stat` show full size with zero downloads; the `dd`
   slice returns the exact source bytes; `rg "objects/" /tmp/cloudstream-server.log`
   shows the transfer was ~10 MB, not the full file; `head`/`cat` stream.
-  Fix whatever surfaces: go-fuse inode/attr handling, `FOPEN_KEEP_CACHE`
   behavior with `dd`, EIO/ENOENT mapping.

### A2. FUSE `create` for brand-new files (closes the last v1 gap)

`CloudDir.Create` currently returns `EROFS`, so `cp file ~/CloudDrive/` and
editor "save as" fail on the mount. Only writes to *existing* files work.

-  Implement `NodeCreater` → call `init-upload` (0 bytes, 0 chunks declared),
-  track bytes written per handle; on `Flush`, tell the control plane the
   final size. The API currently fixes chunk layout at init-upload time, so
   add an endpoint like `POST /files/{id}/commit {size_bytes}` that re-creates
   the chunk table on first real upload (server-side: delete 0-length file
   row and re-init with the real size, or support size-unknown upfront).
-  **Acceptance:** `cp local.txt ~/CloudDrive/x.txt` and `echo hi >>`
   work on a mounted new file; only the touched chunks upload.

### A3. Live OCI end-to-end (validate the PAR backend)

The `oci` backend has never run against real Object Storage.

1. On the VM/server machine: `pip install oci`, create bucket `cloudstream`.
2. Configure `~/.oci/config` + API key; `oci os ns get` to verify.
3. Start with `CLOUDSTREAM_STORAGE=oci CLOUDSTREAM_OCI_BUCKET=cloudstream`.
4. Put a file, range-download, write-back modify it, delete it — all via the
   Go CLI.
-  **Acceptance:** control-plane CPU/network near-zero during transfer; PUT
   PARs succeed despite the `access_list` scoping (falls back gracefully if
   your region rejects it); expired PAR (wait 15 min or set
   `CLOUDSTREAM_PAR_TTL=30`) triggers transparent re-issue, never a failed
   read.
-  Watch for: PAR quota (the service limits outstanding PARs per bucket —
   fine at this scale, but note it), `list_objects` pagination in
   `OCIStorage.delete_file`.

### A4. Deploy the control plane properly

-  TLS: Caddy (automatic Let's Encrypt) or Certbot in front of uvicorn.
-  systemd unit for the FastAPI app (`restart=always`), run with
   `CLOUDSTREAM_STORAGE=oci`, `CLOUDSTREAM_SECRET` from a secrets file.
-  Daily SQLite backup (file copy with WAL checkpoint; it's one file).
-  **Acceptance:** `https://your.domain/healthz` returns ok; reboot the VM and
   the mount still works 60 s later.

## Phase B — Robustness & performance

### B1. Multi-user hardening (M7 depth)

-  Refresh-token rotation on the server (short-access JWT + httponly
   refresh cookie), CLI auto-refresh instead of 24 h token in
   `~/.cloudstream/config.json`.
-  Rename the SQLite WAL setup used by uvicorn's single worker: keep
   `PRAGMA journal_mode=WAL`, add `busy_timeout`.
-  Rate-limit `/auth/*` (e.g. slowapi) — it's the only unauthenticated,
   brute-forceable surface.
-  **Acceptance:** token expires mid-`get` → CLI re-logs-in silently and
   completes; 10 failed logins / min from one IP → 429.

### B2. Prefetch tuning (M5 depth)

Current read-ahead fires one goroutine per sequential `Read` and streams the
next chunk block-by-block. Issues: no concurrency limit, retries hit the next
block of an already-uploaded chunk, and back-to-back sequential reads spawn
duplicate prefetch chains (per-chunk lock saves correctness, not work).

-  Add a throttled prefetch worker: max 2 concurrent chunk-prefetch jobs,
   skip chunks already being fetched, cancel when the app stops reading.
-  Expose `--readahead=KB` flag on mount.
-  **Acceptance:** `mpv`/`ffplay` on a mounted 20 GB file plays smoothly with
   `df`-observable bounded cache growth; prefetch stops within 1 s of pause.

### B3. Write-back conflict check

Before the client PUTs a flushed chunk, pass the *original* checksum (stored
at download time) to a new endpoint `POST /files/{id}/chunks/{n}/prewrite
{expected_checksum}` → returns 409 if the server's copy changed. This is the
foundation for M8.

-  **Acceptance:** device A modifies a chunk; device B's stale write gets 409
   instead of silently clobbering (documented last-writer-wins override flag
   `--force`).

### B4. FUSE polish

-  `statfs` (free space = cache budget or quota), stable `mtime` from DB
   (`updated_at`), `readdirplus`-friendly entry caching keyed by dir TTL
   (already 5 s in-memory — make it configurable).
-  **Acceptance:** `df -h ~/CloudDrive` works; `ls -la --time-style=full-iso`
   shows sane timestamps.

## Phase C — M8 multi-device sync

### C1. Control-plane change-tracking

-  Add `updated_at` filtering: `GET /fs/list?path=/&since=<ts>` returns only
   changed entries; add per-file `revision` bumped on every `complete_chunk`.
-  **Acceptance:** poll from device B shows device A's edit within one poll
   interval (~5 s).

### C2. Client metadata cache invalidation

-  Client keeps the 5 s dir listing cache; on change notification (long-poll
   or short-poll), invalidate just the affected dirs and the chunk masks of
   changed files.
-  **Acceptance:** edit on device A → `ls -la` on device B shows new size
   within 10 s; reads on B of the changed chunk download the new bytes.

## Phase D — Ops & hardening (ongoing)

-  e2e acceptance script: `scripts/e2e.sh` replaying the M1–M6 checks
   (register → mkdir → put → range compare → cache-hit assert → write-back
   compare) so any regression is one command away.
-  Not-built-by-design list (keep it that way in v1): client-side encryption,
   versioning, Windows/WinFsp, smart eviction beyond LRU.
-  Optional later: OCI native multipart for chunks > 4 GiB (single PUT limit
   — 64 MB chunks are fine today).

---

## Suggested order

```
A1 (mount acceptance)  →  A2 (create)  →  A3 (live OCI)  →  A4 (deploy)
      ↓
B1 (auth)  →  B2 (prefetch)  →  B3 (prewrite check)  →  B4 (fuse polish)
      ↓
C1 → C2 (multi-device)
```