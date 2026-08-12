// Package chunk implements the core data path: mapping reads/writes onto
// 64 MB chunks, fetching blocks over HTTP Range requests against
// pre-authenticated URLs, caching them, and uploading dirty chunks back.
package chunk

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloudstream/internal/api"
	"cloudstream/internal/cache"
)

const (
	parRetries = 3
	blockSize  = cache.BlockSize
)

type Manager struct {
	API     *api.Client
	Cache   *cache.Cache
	Staging string // root dir for write-back staging files

	// ReadAhead controls prefetching of chunks following sequential reads.
	ReadAhead bool

	mu      sync.Mutex
	chunkMu map[string]*sync.Mutex // per (file,chunk) fetch serialization
}

func NewManager(apiclient *api.Client, c *cache.Cache, staging string) *Manager {
	m := &Manager{
		API:       apiclient,
		Cache:     c,
		Staging:   staging,
		ReadAhead: true,
		chunkMu:   map[string]*sync.Mutex{},
	}
	if err := os.MkdirAll(staging, 0o755); err != nil {
		panic(err)
	}
	return m
}

func (m *Manager) chunkLock(fileID string, idx int) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := fileID + ":" + fmt.Sprintf("%06d", idx)
	l, ok := m.chunkMu[k]
	if !ok {
		l = &sync.Mutex{}
		m.chunkMu[k] = l
	}
	return l
}

// ---------------------------------------------------------------------------
// File (open handle over a remote file)
// ---------------------------------------------------------------------------

type File struct {
	m     *Manager
	Meta  *api.FileMeta
	mu    sync.Mutex
	last  int64 // last sequential read end offset
	stg   map[int][]bool
	dirty map[int]bool
}

// OpenPath resolves a virtual path to its metadata and returns a file handle.
func (m *Manager) OpenPath(ctx context.Context, path string) (*File, error) {
	meta, err := m.Resolve(ctx, path)
	if err != nil {
		return nil, err
	}
	return &File{m: m, Meta: meta, stg: map[int][]bool{}, dirty: map[int]bool{}}, nil
}

// Resolve walks /fs/list from the root, using the file id the control plane
// now includes in listings, and returns the file's full metadata.
func (m *Manager) Resolve(ctx context.Context, path string) (*api.FileMeta, error) {
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid path %q", path)
	}
	dir := "/"
	for _, seg := range parts {
		list, err := m.API.List(ctx, dir)
		if err != nil {
			return nil, err
		}
		var found *api.DirEntry
		for i := range list.Entries {
			if list.Entries[i].Name == seg {
				found = &list.Entries[i]
				break
			}
		}
		if found == nil {
			return nil, fmt.Errorf("path not found: %s", path)
		}
		if found.Type == "file" {
			meta, err := m.API.FileMeta(ctx, found.ID)
			if err != nil {
				return nil, err
			}
			return meta, nil
		}
		if dir == "/" {
			dir = "/" + seg
		} else {
			dir = dir + "/" + seg
		}
	}
	return nil, fmt.Errorf("path is a directory: %s", path)
}

// ---------------------------------------------------------------------------
// Read path
// ---------------------------------------------------------------------------

// ReadAt fills p with bytes from the remote file at off. Only the blocks
// actually intersecting the request are fetched; everything else is served
// from the local cache.
func (f *File) ReadAt(ctx context.Context, p []byte, off int64) (int, error) {
	meta := f.Meta
	if off >= meta.SizeBytes {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	want := int64(len(p))
	if remain := meta.SizeBytes - off; want > remain {
		want = remain
	}
	read := int64(0)
	for read < want {
		rel := off + read
		ci := int(rel / meta.ChunkSize)
		within := rel % meta.ChunkSize
		chunkBytes := f.blockSize(ci)
		avail := chunkBytes - within // bytes left in this chunk
		take := want - read
		if take > avail {
			take = avail
		}
		blk := int(within / blockSize)
		served := int64(0)
		for served < take {
			data, err := f.block(ctx, ci, blk)
			if err != nil {
				return int(read), err
			}
			start := within + served - int64(blk)*blockSize
			room := int64(len(data)) - start
			if room <= 0 {
				return int(read), fmt.Errorf("corrupt cache block chunk=%d block=%d", ci, blk)
			}
			n2 := take - served
			if n2 > room {
				n2 = room
			}
			copy(p[read+served:read+served+n2], data[start:start+n2])
			served += n2
			blk++
		}
		read += take
	}
	f.noteSequential(ctx, off, read)
	return int(read), nil
}

// blockSize returns the byte length of a chunk.
func (f *File) blockSize(ci int) int64 {
	if ci == f.Meta.ChunkCount-1 {
		return f.Meta.Chunks[ci].SizeBytes
	}
	return f.Meta.ChunkSize
}

func (f *File) numBlocks(ci int) int {
	return numBlocksSize(f.blockSize(ci))
}

// block returns the bytes of block blk in chunk ci, from cache or network.
func (f *File) block(ctx context.Context, ci, blk int) ([]byte, error) {
	nblk := f.numBlocks(ci)
	if blk < 0 || blk >= nblk {
		return nil, fmt.Errorf("block %d out of range (chunk %d has %d)", blk, ci, nblk)
	}
	if data, ok := f.m.Cache.GetBlock(f.Meta.ID, ci, blk); ok {
		return data, nil
	}
	cache.Misses.Add(1)
	lock := f.m.chunkLock(f.Meta.ID, ci)
	lock.Lock()
	defer lock.Unlock()
	if data, ok := f.m.Cache.GetBlock(f.Meta.ID, ci, blk); ok {
		return data, nil
	}
	cache.Hits.Add(1)
	start := int64(blk) * blockSize
	length := f.blockSize(ci) - start
	if length > blockSize {
		length = blockSize
	}
	data, err := f.fetchRange(ctx, ci, start, length)
	if err != nil {
		return nil, err
	}
	if err := f.m.Cache.PutBlock(f.Meta.ID, ci, blk, data, nblk); err != nil {
		return nil, err
	}
	return data, nil
}

// fetchRange GETs bytes [start, start+length) of a chunk, refreshing the
// PAR on each retry so expires_at mid-transfer is transparent.
func (f *File) fetchRange(ctx context.Context, ci int, start, length int64) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < parRetries; attempt++ {
		url, err := f.m.API.DownloadURL(ctx, f.Meta.ID, ci)
		if err != nil {
			lastErr = err
			continue
		}
		data, err := httpRange(url, start, length)
		if err == nil {
			return data, nil
		}
		lastErr = err
		if !retryable(err) {
			break
		}
		time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
	}
	return nil, fmt.Errorf("chunk %d [%d,%d): %w", ci, start, start+length, lastErr)
}

func httpRange(url string, start, length int64) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if length > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+length-1))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return nil, &httpStatusError{code: resp.StatusCode}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, length+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("short read: got %d want %d", len(data), length)
	}
	return data, nil
}

type httpStatusError struct{ code int }

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("HTTP %d", e.code)
}

// retryable reports whether a failed HTTP range fetch is worth a retry with
// a freshly issued PAR: auth failures (PAR expiry), throttling, and 5xx.
// 4xx like 404/416 are fatal.
func retryable(err error) bool {
	var he *httpStatusError
	if ok := errorAs(err, &he); ok {
		switch {
		case he.code == http.StatusUnauthorized, he.code == http.StatusForbidden:
			return true
		case he.code == http.StatusTooManyRequests:
			return true
		case he.code >= 500:
			return true
		default:
			return false
		}
	}
	return true // network errors: retry with a fresh URL
}

func errorAs(err error, target *(*httpStatusError)) bool {
	for err != nil {
		if he, ok := err.(*httpStatusError); ok {
			*target = he
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// noteSequential tracks read position and triggers background prefetch of
// the chunk following the one just finished, but only for sequential runs.
func (f *File) noteSequential(ctx context.Context, off, read int64) {
	f.mu.Lock()
	sequential := off == f.last
	if sequential {
		f.last = off + read
	}
	f.mu.Unlock()
	if !sequential || !f.m.ReadAhead {
		return
	}
	lastChunk := (off + read - 1) / f.Meta.ChunkSize
	if lastChunk+1 < int64(f.Meta.ChunkCount) {
		go f.prefetch(ctx, int(lastChunk+1))
	}
}

// prefetch streams the blocks of one chunk into the cache in the background
// (cooperative: one block fetch at a time, serialized per chunk).
func (f *File) prefetch(ctx context.Context, ci int) {
	lock := f.m.chunkLock(f.Meta.ID, ci)
	lock.Lock()
	defer lock.Unlock()
	nblk := f.numBlocks(ci)
	for b := 0; b < nblk && ctx.Err() == nil; b++ {
		if _, ok := f.m.Cache.GetBlock(f.Meta.ID, ci, b); ok {
			continue
		}
		start := int64(b) * blockSize
		length := f.blockSize(ci) - start
		if length > blockSize {
			length = blockSize
		}
		data, err := f.fetchRange(ctx, ci, start, length)
		if err != nil {
			return
		}
		if err := f.m.Cache.PutBlock(f.Meta.ID, ci, b, data, nblk); err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Write path
// ---------------------------------------------------------------------------

// WriteAt records bytes into the write-back staging area on local disk and
// returns immediately. Upload happens on Flush.
func (f *File) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	written := 0
	for written < len(p) {
		ci := int(off / f.Meta.ChunkSize)
		within := off % f.Meta.ChunkSize
		room := f.blockSize(ci) - within
		if room <= 0 {
			return written, fmt.Errorf("write beyond EOF at offset %d", off)
		}
		n := int64(len(p) - written)
		if n > room {
			n = room
		}
		if err := f.stageChunk(ci, within, p[written:written+int(n)]); err != nil {
			return written, err
		}
		written += int(n)
		off += n
	}
	return written, nil
}

// stageChunk writes a dirty range into a chunk's staging file and marks the
// covered blocks dirty (for the merge-on-flush step).
func (f *File) stageChunk(ci int, within int64, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := f.stagePath(ci)
	os.MkdirAll(filepath.Dir(path), 0o755)
	sf, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer sf.Close()
	if _, err := sf.WriteAt(data, within); err != nil {
		return err
	}
	if f.stg[ci] == nil {
		f.stg[ci] = make([]bool, f.numBlocks(ci))
	}
	first := int(within / blockSize)
	spanEnd := (within + int64(len(data)) - 1) / blockSize
	for b := first; b <= int(spanEnd); b++ {
		f.stg[ci][b] = true
	}
	f.dirty[ci] = true
	return nil
}

func (f *File) stagePath(ci int) string {
	return filepath.Join(f.m.Staging, f.Meta.ID, fmt.Sprintf("chunk_%06d.staging", ci))
}

// Flush uploads every dirty chunk: it merges staged bytes over the original
// chunk content (downloads it if not cached), PUTs the whole chunk through a
// fresh upload URL, then marks the chunk complete on the control plane.
func (f *File) Flush(ctx context.Context) error {
	f.mu.Lock()
	dirty := make([]int, 0, len(f.dirty))
	for ci := range f.dirty {
		dirty = append(dirty, ci)
	}
	f.mu.Unlock()
	for _, ci := range dirty {
		if err := f.uploadChunk(ctx, ci); err != nil {
			return fmt.Errorf("chunk %d: %w", ci, err)
		}
	}
	return nil
}

func (f *File) uploadChunk(ctx context.Context, ci int) error {
	f.mu.Lock()
	mask := f.stg[ci]
	f.mu.Unlock()

	chunkBytes := f.blockSize(ci)
	base, err := f.readFullChunk(ctx, ci)
	if err != nil {
		return err
	}

	staged, err := os.ReadFile(f.stagePath(ci))
	if err != nil {
		return err
	}
	for b, resident := range mask {
		if !resident {
			continue
		}
		start := int64(b) * blockSize
		length := start + blockSize
		if length > chunkBytes {
			length = chunkBytes
		}
		seg := staged[start:length]
		if int64(len(seg)) != length-start {
			return fmt.Errorf("staging truncated in chunk %d block %d", ci, b)
		}
		copy(base[start:length], seg)
	}

	var lastErr error
	for attempt := 0; attempt < parRetries; attempt++ {
		url, err := f.m.API.UploadURL(ctx, f.Meta.ID, ci)
		if err != nil {
			lastErr = err
			continue
		}
		if err := httpPut(url, base); err != nil {
			lastErr = err
			if !retryable(err) {
				break
			}
			time.Sleep(time.Duration(200*(attempt+1)) * time.Millisecond)
			continue
		}
		lastErr = nil
		break
	}
	if lastErr != nil {
		return lastErr
	}

	if err := f.m.API.CompleteChunk(ctx, f.Meta.ID, ci, md5hex(base), chunkBytes); err != nil {
		return err
	}

	f.mu.Lock()
	delete(f.dirty, ci)
	f.stg[ci] = nil
	f.mu.Unlock()
	os.RemoveAll(f.stagePath(ci))
	return nil
}

func (f *File) readFullChunk(ctx context.Context, ci int) ([]byte, error) {
	chunkBytes := f.blockSize(ci)
	out := make([]byte, chunkBytes)
	nblk := f.numBlocks(ci)
	for b := 0; b < nblk; b++ {
		data, err := f.block(ctx, ci, b)
		if err != nil {
			return nil, err
		}
		start := int64(b) * blockSize
		copy(out[start:], data)
	}
	return out, nil
}

func httpPut(url string, data []byte) error {
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return &httpStatusError{code: resp.StatusCode}
	}
	return nil
}

func md5hex(b []byte) string {
	h := md5.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func numBlocksSize(chunkBytes int64) int {
	n := int((chunkBytes + blockSize - 1) / blockSize)
	if n < 1 {
		n = 1
	}
	return n
}

func splitPath(p string) []string {
	var out []string
	for _, seg := range strings.Split(p, "/") {
		if seg != "" {
			out = append(out, seg)
		}
	}
	return out
}

func parentOf(p string) string {
	parts := splitPath(p)
	if len(parts) <= 1 {
		return "/"
	}
	return "/" + strings.Join(parts[:len(parts)-1], "/")
}