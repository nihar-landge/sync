# Status — Done & Pushed vs Remaining

Last updated: 2026-08-13 · Repo: github.com/nihar-landge/sync (branch `master`, 1 commit, 25 files, 3036 lines)

Milestone legend from the build guide: M1 API → M2 PARs → M3 Go chunk
manager + cache → M4 FUSE → M5 read-ahead → M6 write-back → M7 auth/PAR
refresh → M8 multi-device.

---

## Part 1 — Done and pushed (in the GitHub repo)

### 1.1 Server (control plane) — `server/` — M1, M2, M7(partial)

| File | What it implements |
|---|---|
| `app/main.py` | FastAPI app, healthz, route wiring |
| `app/config.py` | env config: DB/object dirs, JWT secret/TTL, chunk size (64 MB), PAR TTL (15 min), storage backend switch, OCI vars |
| `app/db.py` | SQLite schema (users, files, chunks, directories, WAL), all DAO/query logic, virtual-path tree handling |
| `app/auth.py` | PBKDF2 password hashing, HS256 JWT issue/verify, `get_current_user` dependency |
| `app/storage.py` | Two backends behind one interface: `LocalStorage` (dev stand-in: signed expiring object URLs, same shape as a PAR) and `OCIStorage` (real Pre-Authenticated Requests, write PARs scoped to one object via `access_list`, falls back unscoped if a region rejects it), delete via `delete_object` |
| `app/routes/auth.py` | `POST /auth/register`, `POST /auth/login` → JWT |
| `app/routes/fs.py` | `GET /fs/list?path=` (entries include file id), `POST /fs/mkdir` |
| `app/routes/files.py` | `init-upload` (creates file row + chunk table), per-chunk `upload-url` / `download-url`, `complete` (checksum + marks uploaded, auto-flips file status to `complete`), `meta`, `DELETE` |
| `app/routes/objects.py` | local-backend object GET (Range-aware via Starlette FileResponse) and PUT; HMAC-token validation with expiry — this is the dev-mode PAR stand-in |
| `requirements.txt` | fastapi, uvicorn, PyJWT (+ commented `oci` for production) |

Design point honored: the control plane never proxies bytes; it only issues
short-lived direct-access URLs.

### 1.2 Client — `client/` — M3, M4(code), M5, M6, M7(partial)

| File | What it implements |
|---|---|
| `internal/api/api.go` | Typed REST client (login, list, mkdir, init-upload, PAR urls, complete, meta, delete) |
| `internal/cache/cache.go` | Disk cache at `~/.cloudstream/cache/<file_id>/chunk_<n>`, sparse chunk files, 256 KiB resident-block masks persisted as `.mask.json` (cache survives restarts), LRU index via `hashicorp/golang-lru/v2`, byte-budget eviction (default 5 GiB), hit/miss counters, cache rebuild from disk on startup |
| `internal/chunk/chunk.go` | `ReadAt`: offset → (chunk, block) mapping, fetch-only-requested-blocks, per-chunk singleflight fetch, PAR re-issue + retry on 401/403/5xx (expiry-transparent), sequential read-ahead prefetch worker; `WriteAt`: local staging files, dirty-block masks; `Flush`: merge staged blocks over original chunk → PUT whole chunk → `complete` (only dirty chunks re-upload); `Resolve`: path→id via listings → full meta |
| `internal/fuse/fs.go`, `internal/fuse/file.go` | go-fuse v2 filesystem: `Lookup`, `Readdir`, `Getattr`, `Mkdir`, `Open`, `Read`, `Write`, `Flush`; 5 s in-memory dir listing cache; stable hashed inodes |
| `cmd/cloudstream/main.go` | CLI: `login ls mkdir put get cat rm stat mount cache`; config at `~/.cloudstream/config.json`; flags may appear after subcommand |
| `internal/chunk/chunk_test.go` | 5 unit tests (see verified results) |

### 1.3 Reproduction & docs (pushed)

- `README.md` — architecture, quickstart (dev + OCI), design notes, milestone table, API surface, next-steps link
- `NEXT_STEPS.md` — ordered backlog A→D with acceptance criteria
- `scripts/dev.sh` — start/stop the dev control plane
- `.gitignore` — excludes venv, binary, caches

### 1.4 Actually verified (evidence from the build session)

| Check | Result |
|---|---|
| M1: 256 MB upload via chunk URLs + 111-byte mid-file slice | `206 Partial Content`, `content-range: bytes 15782272-15782382/67108864`, byte-diff vs source: **PASS** |
| M1: delete file + chunks | list empty, objects dir cleaned: **PASS** |
| M2 shape: PUT/GET of objects out-of-band of JWT (signed URL only) | PUT=200 for all 4 chunks, object served without auth header: **PASS** |
| M3: `cloudstream put` 256 MB (4 chunks) | chunk 1–4 uploaded, `stat` shows `status: complete`, id `4cfd…` |
| M3: `get` #1 vs source | `cmp` **byte-identical** |
| M3: cache persistence across restarts | 256 MiB resident rebuilt from disk masks, 4 chunks |
| M3: `get` #2 and #3 entirely from cache | server log `GET /api/v1/objects/` went 1024 → 1024 (0 new requests, 0 bytes network): **PASS** |
| M3: `cat \| md5` | `218fe448…` == source md5: **PASS** |
| M3: `rm` | file + cache dirs removed: **PASS** |
| Unit: read spanning 64 MB chunk boundary | bytes exact, second read 0 network requests |
| Unit: block granularity ("only what you touch") | 3 reads in 3 different 256 KiB blocks → exactly 3 range requests |
| Unit: EOF behavior | `io.EOF` with 0 bytes at offset == size |
| Unit: write-back round trip | overwrite 1 MB mid-file → flush → PUT body matches source-with-modification at every byte |
| Unit: LRU eviction | 8 chunks into a 3-block budget → ≤3 chunks retained, newest survives |
| Go toolchain | `go build ./...`, `go vet ./...`, `go test ./...` all clean |
| FUSE without macFUSE | mounts fail with clean `no FUSE mount utility found` (expected on this Mac) |

---

## Part 2 — Remaining (not built / not yet run)

### 2.1 Build-stage gaps (code exists but never exercised or missing)

| # | Item | Milestone | What's missing |
|---|---|---|---|
| R1 | Real FUSE mount acceptance | M4 | FUSE layer never ran against a real kernel mount. Needs macFUSE (macOS) or fuse3 (Linux), then `mount`, `ls`, `dd skip=` slice, and network-transfer measurement (~10 MB for a 10 MB dd, not the whole file). Compile-clean today; runtime behavior unproven (inode/attr handling, FOPEN_KEEP_CACHE, EIO mapping). |
| R2 | FUSE `create` of new files | M6 gap | `CloudDir.Create` returns `EROFS`. `cp file mount/dir/`, editor "save as", and `echo >> newfile` do NOT work through the mount. Needs a size-unknown upload path (e.g. `POST /files/{id}/commit {size}` or init-with-lazy-size) and wiring `NodeCreater`. |
| R3 | Live OCI run | M2 | The `oci` backend has never talked to real Object Storage (no `~/.oci/config`, no bucket, `oci` SDK not installed). Must validate PAR creation, `access_list` scoping, PAR-quota behavior, expiry refresh, near-zero control-plane load during transfers. |
| R4 | Full server test suite | M1 | No pytest suite; only curl-level manual verification. Regression harness (`scripts/e2e.sh`) not written. |
| R5 | Deployment | ops | No TLS (Caddy/Certbot), no systemd unit, no SQLite backup cron, no secrets management for `CLOUDSTREAM_SECRET`, no `busy_timeout`/WAL tuning under load. |

### 2.2 Hardening / features (from NEXT_STEPS.md)

| # | Item | Milestone | What's missing |
|---|---|---|---|
| R6 | Refresh-token rotation, CLI silent re-auth | M7 | 24 h JWT stored in `~/.cloudstream/config.json`; expiry mid-`get` is handled for PARs only, not the session token. Rate limiting on `/auth/*` missing. |
| R7 | Prefetch tuning | M5 | Unbounded prefetch goroutines (one per sequential Read), no cancel-on-pause, no `--readahead` flag; untested with video playback (`mpv` acceptance pending R1). |
| R8 | Pre-write checksum check (`/prewrite`) | M8 base | None — stale device B writes silently clobber device A's newer bytes. |
| R9 | Multi-device sync | M8 | No `since=` list filtering, no per-file revision, no client invalidation polling. Entirely not built (by design for v1). |
| R10 | FUSE polish | ops | No `statfs` (df), mtime is DB-only (`updated_at`), dir-listing TTL hard-coded at 5 s. |
| R11 | OCI multipart for huge chunks | stretch | Single-PUT chunks are fine ≤4 GiB; multipart/upload-manager noted as a later optimization. |

### 2.3 Known v1 limitations (intentional, documented)

- Client-side encryption: no — relies on OCI server-side encryption.
- Versioning/history: none; last-writer-wins only.
- Windows/WinFsp: not targeted.
- Cache eviction: plain LRU only.
- `fs/list` unpaginated (fine at single-user scale).
- New files via `put` require `size_bytes` upfront (256 MB→4 chunks test used it); the FUSE `create` gap is R2.

---

## Part 3 — Suggested execution order

```
R1 mount acceptance ──► R2 FUSE create ──► R3 live OCI ──► R5 deploy (+R4 e2e script)
        │
        └──► R6 auth hardening ──► R7 prefetch ──► R8 prewrite check ──► R10 polish
                                                                          │
                                                          R9 multi-device (M8)
```

Done-and-pushed today: M1, M2 (local stand-in + OCI code), M3, M4 (code),
M5 (code + unit), M6 (unit-verified), M7 (PAR refresh + JWT basics).
Pushed: 1 commit (`6c3ab1a`) on `master`.