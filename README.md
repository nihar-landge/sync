# CloudStreamFS

A FUSE-mounted filesystem where file *metadata* lives in SQLite on a control
plane (FastAPI on an Oracle Micro VM), and file *content* streams on-demand
from OCI Object Storage via chunked HTTP range reads. The control plane never
proxies bytes — it hands out short-lived pre-authenticated URLs (OCI PARs) and
the client talks to Object Storage directly.

```bash
cloudstream mount ~/CloudDrive
ls -lh ~/CloudDrive/movies/movie.mkv    # 20G reported, 0 bytes downloaded
dd if=~/CloudDrive/movies/movie.mkv bs=1M skip=500 count=10   # fetches ~10 MB
```

## Layout

```
server/   FastAPI control plane (SQLite metadata, JWT auth, PAR issuance)
  app/            config, db, auth, storage (local + OCI), routes
client/   Go client (CLI + FUSE)
  cmd/cloudstream/    CLI: login ls mkdir put get cat rm stat mount cache
  internal/api/       control-plane REST client
  internal/cache/     disk-backed LRU chunk cache (block-granular, byte-capped)
  internal/chunk/     read/write path: range math, PAR fetch+retry, read-ahead,
                      write-back staging and merge-on-flush
  internal/fuse/      go-fuse v2 filesystem (Lookup/Readdir/Getattr/Read/Write/
                      Flush)
scripts/dev.sh        start/stop the dev control plane
```

## Quickstart (dev mode)

The default `local` storage backend stands in for OCI Object Storage: chunks
live on the server disk and are served through the API with a signed,
expiring token — the same URL contract as a PAR, so switching to OCI requires
zero client changes.

```bash
# 1. control plane
cd server
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
../scripts/dev.sh start          # http://127.0.0.1:8000

# 2. client
cd client
go build -o cloudstream ./cmd/cloudstream
./cloudstream --api http://127.0.0.1:8000 login --user dev --pass password123

# 3. use it
./cloudstream mkdir /movies
./cloudstream put ~/bigfile.mkv /movies/bigfile.mkv
./cloudstream stat /movies/bigfile.mkv        # size/chunk map from control plane
./cloudstream get /movies/bigfile.mkv /tmp/out.mkv
./cloudstream rm /movies/bigfile.mkv
./cloudstream cache                            # LRU hits/misses, resident bytes

# 4. FUSE (M4+)
#    Linux: sudo apt install fuse3 libfuse3-dev
#    macOS: install macFUSE
./cloudstream mount ~/CloudDrive
```

## Production (OCI)

1. Create an OCI bucket and an API key + `~/.oci/config` (test with
   `oci os ns get`).
2. On the VM: `pip install oci`, then run with:
   ```
   CLOUDSTREAM_STORAGE=oci \
   CLOUDSTREAM_OCI_BUCKET=cloudstream \
   CLOUDSTREAM_URL=https://your.domain \
   CLOUDSTREAM_SECRET=<random> \
   uvicorn app.main:app --host 0.0.0.0 --port 8000
   ```
3. Terminate TLS (Caddy/Certbot) in front of it. Object keys are
   `file_<id>/chunk_<n>`; every read/write goes through a PAR the API issued
   (15-minute TTL, reissued transparently on expiry), never a public bucket.

## Design notes

- **Chunking**: fixed 64 MB chunks; block-granular (256 KiB) range fetches so
  a 10 MB `dd` out of a 20 GB video transfers ~10 MB, not 64 MB, and each
  cached chunk file is sparse with a resident-block mask persisted to disk
  (cache survives restarts; LRU evicts whole chunks by byte budget, default
  5 GiB).
- **Read path**: `ReadAt` maps offset→(chunk, block), pulls only the blocks
  intersecting the request (PARs are re-issued and retried on 401/403/5xx),
  serves the rest from cache, and spawns a background prefetch of the next
  chunk when the access pattern is sequential.
- **Write path**: writes stage to local disk immediately (`Flush` uploads).
  A dirty chunk is merged (staged blocks over the original bytes, downloaded
  if not cached) and PUT whole through a fresh upload URL — only the touched
  chunk(s) re-upload, never the whole file. Staging survives restarts; a
  failed upload keeps the chunk dirty for retry.
- **v1 security**: HTTPS + JWT + per-object short-TTL PARs; server-side
  encryption at rest via OCI. Not built: client-side encryption, versioning,
  Windows FUSE, custom resumable protocol (use OCI multipart later).

## Milestones

| Milestone | Status | Verification |
|---|---|---|
| M1 API + chunked upload/download + ranges | done | 206 Partial Content, byte-diff of mid-file slice |
| M2 PAR-based direct access (local stand-in) | done | client PUTs/GETs objects; control plane CPU idle during transfer |
| M3 Go chunk manager + LRU cache | done | 3rd `get` of 256 MB → 0 object requests (`rg "objects/" /tmp/cloudstream-server.log`) |
| M4 FUSE mount | done* | compiles; graceful error without macFUSE; test `dd` slice on a mounted box |
| M5 sequential read-ahead | done | background prefetch on sequential reads (see `chunk.noteSequential`) |
| M6 write-back + merge-on-flush | done | unit-tested round trip: modified chunk re-uploaded, others untouched |
| M7 JWT + PAR expiry refresh | done | transparent PAR re-issue on 401/403 (`fetchRange` retries) |
| M8 multi-device sync | not built | — |

*M4 full acceptance (mount + `dd` on device) requires macFUSE/fuse3 on the test machine.

## API surface

```
POST /api/v1/auth/register|login         # JWT
GET  /api/v1/fs/list?path=/movies        # entries incl. file id
POST /api/v1/fs/mkdir                    # {path}
POST /api/v1/files/init-upload           # {path, size_bytes} → {file_id, chunk map}
GET  /api/v1/files/{id}/chunks/{n}/upload-url   # PAR for PUT (any chunk, incl. rewrite)
GET  /api/v1/files/{id}/chunks/{n}/download-url # PAR for GET
POST /api/v1/files/{id}/chunks/{n}/complete     # {checksum, size_bytes}
GET  /api/v1/files/{id}/meta
DELETE /api/v1/files/{id}
```