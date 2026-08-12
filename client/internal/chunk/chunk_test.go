package chunk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"cloudstream/internal/api"
	"cloudstream/internal/cache"
)

// fakeServer stands in for both the control plane and the object store.
// Data is generated deterministically so expected bytes are computable.
type fakeServer struct {
	*httptest.Server
	data    []byte
	chunk   int64
	gets    atomic.Int64
	puts    atomic.Int64
	putBody map[int][]byte
}

func newFakeServer(size int64, chunkSize int64) *fakeServer {
	fs := &fakeServer{chunk: chunkSize, putBody: map[int][]byte{}}
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*31 + 7) % 251)
	}
	fs.data = data
	fs.Server = httptest.NewServer(http.HandlerFunc(fs.handle))
	return fs
}

func (fs *fakeServer) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/api/v1/files/f1/meta":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "f1", "path": "/movies/m.bin", "size_bytes": len(fs.data),
			"chunk_size": fs.chunk, "chunk_count": (len(fs.data)+int(fs.chunk)-1)/int(fs.chunk),
			"status": "complete",
			"chunks": chunkList(fs.data, fs.chunk),
		})
	case r.URL.Path == "/api/v1/fs/list":
		w.Header().Set("Content-Type", "application/json")
		dir := r.URL.Query().Get("path")
		var entries []map[string]any
		if dir == "" || dir == "/" {
			entries = []map[string]any{
				{"name": "movies", "type": "dir", "size": 0},
				{"id": "f1", "name": "m.bin", "type": "file", "size": len(fs.data)},
			}
		} else if dir == "/movies" {
			entries = []map[string]any{
				{"id": "f1", "name": "m.bin", "type": "file", "size": len(fs.data)},
			}
		}
		json.NewEncoder(w).Encode(map[string]any{
			"path": dir, "parent": "", "entries": entries,
		})
	case strings.HasPrefix(r.URL.Path, "/api/v1/files/f1/chunks/") && strings.HasSuffix(r.URL.Path, "/download-url"):
		var n int
		fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/files/f1/chunks/"), "/download-url"), "%d", &n)
		json.NewEncoder(w).Encode(map[string]string{"url": fmt.Sprintf("%s/obj?n=%d", fs.URL, n), "expires_at": "1"})
	case strings.HasPrefix(r.URL.Path, "/api/v1/files/f1/chunks/") && strings.HasSuffix(r.URL.Path, "/upload-url"):
		var n int
		fmt.Sscanf(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/files/f1/chunks/"), "/upload-url"), "%d", &n)
		json.NewEncoder(w).Encode(map[string]string{"url": fmt.Sprintf("%s/obj?n=%d", fs.URL, n), "expires_at": "1"})
	case strings.HasPrefix(r.URL.Path, "/api/v1/files/f1/chunks/") && strings.HasSuffix(r.URL.Path, "/complete"):
		io.Copy(io.Discard, r.Body)
		w.Write([]byte(`{"ok":true}`))
	case r.URL.Path == "/obj":
		if r.Method == http.MethodPut {
			fs.puts.Add(1)
			var n int
			fmt.Sscanf(r.URL.Query().Get("n"), "%d", &n)
			body, _ := io.ReadAll(r.Body)
			fs.putBody[n] = body
			w.WriteHeader(200)
			w.Write([]byte(`{"ok":true}`))
			return
		}
		// object-store stand-in with Range support; each chunk is its own object
		fs.gets.Add(1)
		var n int
		fmt.Sscanf(r.URL.Query().Get("n"), "%d", &n)
		objStart := int64(n) * fs.chunk
		objLen := int64(len(fs.data)) - objStart
		if objLen > fs.chunk {
			objLen = fs.chunk
		}
		start, end := 0, int(objLen)-1
		rangeHdr := r.Header.Get("Range")
		if rangeHdr != "" {
			fmt.Sscanf(rangeHdr, "bytes=%d-%d", &start, &end)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, objLen))
			w.Header().Set("Accept-Ranges", "bytes")
			w.WriteHeader(http.StatusPartialContent)
		}
		w.Write(fs.data[objStart+int64(start) : objStart+int64(end)+1])
	}
}

func chunkList(data []byte, chunkSize int64) []map[string]any {
	n := 0
	for i := 0; i < len(data); i += int(chunkSize) {
		n++
	}
	out := make([]map[string]any, 0, n)
	offset := 0
	for i := 0; i < n; i++ {
		remain := len(data) - offset
		sz := int64(chunkSize)
		if int64(remain) < sz {
			sz = int64(remain)
		}
		out = append(out, map[string]any{
			"index": i, "object_key": fmt.Sprintf("file_f1/chunk_%06d", i),
			"size_bytes": sz, "uploaded": true, "checksum": "",
		})
		offset += int(sz)
	}
	return out
}

func newTestManager(t *testing.T, fs *fakeServer) (*Manager, *File) {
	t.Helper()
	dir := t.TempDir()
	apic := api.New(fs.URL, "tok")
	c, err := cache.New(filepath.Join(dir, "cache"), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	m := NewManager(apic, c, filepath.Join(dir, "staging"))
	m.ReadAhead = false
	f, err := m.OpenPath(context.Background(), "/movies/m.bin")
	if err != nil {
		t.Fatal(err)
	}
	return m, f
}

func TestReadAcrossChunkBoundary(t *testing.T) {
	fs := newFakeServer(100*1024*1024, 64*1024*1024)
	defer fs.Close()
	_, f := newTestManager(t, fs)

	ctx := context.Background()
	// Range that spans the first/second chunk boundary, unaligned at both ends.
	off := int64(64*1024*1024 - 1000)
	buf := make([]byte, 3000)
	n, err := f.ReadAt(ctx, buf, off)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3000 {
		t.Fatalf("read %d want 3000", n)
	}
	for i := 0; i < n; i++ {
		if buf[i] != fs.data[off+int64(i)] {
			t.Fatalf("byte %d: got %d want %d", i, buf[i], fs.data[off+int64(i)])
		}
	}

	// Second identical read must be served entirely from cache.
	misses := fs.gets.Load()
	buf2 := make([]byte, 3000)
	if _, err := f.ReadAt(ctx, buf2, off); err != nil {
		t.Fatal(err)
	}
	if got := fs.gets.Load(); got != misses {
		t.Fatalf("second read made %d network requests (first saw %d)", got-misses, misses)
	}
}

func TestReadHitsCacheNotNetwork(t *testing.T) {
	fs := newFakeServer(3*1024*1024, 64*1024*1024)
	defer fs.Close()
	_, f := newTestManager(t, fs)
	ctx := context.Background()

	// Three read slices, each inside a different 256 KB block of chunk 0.
	for pass, off := range []int64{0, cache.BlockSize + 100, 2 * cache.BlockSize + 100} {
		buf := make([]byte, 4096)
		n, err := f.ReadAt(ctx, buf, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != 4096 {
			t.Fatalf("pass %d read %d", pass, n)
		}
	}
	if got := fs.gets.Load(); got != 3 {
		t.Fatalf("expected exactly 3 range requests (one per block), got %d", got)
	}
	// Every request transferred one 256KB block.
	if c, _ := f.m.Cache.Stats(); c == 0 {
		t.Fatal("cache empty after reads")
	}
}

func TestReadEmptyAndEOF(t *testing.T) {
	fs := newFakeServer(1*1024*1024, 64*1024*1024)
	defer fs.Close()
	_, f := newTestManager(t, fs)
	ctx := context.Background()

	buf := make([]byte, 10)
	if _, err := f.ReadAt(ctx, buf, 0); err != nil {
		t.Fatal(err)
	}
	n, err := f.ReadAt(ctx, buf, int64(len(fs.data)))
	if err == nil || n != 0 {
		t.Fatalf("EOF read: n=%d err=%v", n, err)
	}
}

func TestWriteBackRoundTrip(t *testing.T) {
	fs := newFakeServer(3*1024*1024, 64*1024*1024)
	defer fs.Close()
	_, f := newTestManager(t, fs)
	ctx := context.Background()

	// Overwrite 1 MB in the middle.
	off := int64(1024 * 1024)
	newData := make([]byte, 1024*1024)
	for i := range newData {
		newData[i] = 0xAB
	}
	if n, err := f.WriteAt(newData, off); err != nil || n != len(newData) {
		t.Fatalf("WriteAt: n=%d err=%v", n, err)
	}
	if err := f.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if fs.puts.Load() < 1 || len(fs.putBody[0]) == 0 {
		t.Fatal("flush did not upload chunk")
	}
	got := fs.putBody[0]
	for i := 0; i < len(fs.data); i++ {
		want := byte(fs.data[i])
		if int64(i) >= off && int64(i) < off+int64(len(newData)) {
			want = newData[int64(i)-off]
		}
		if got[i] != want {
			t.Fatalf("byte %d: got %d want %d", i, got[i], want)
		}
	}
}

func TestCacheEvictionCapsBytes(t *testing.T) {
	dir := t.TempDir()
	c, err := cache.New(filepath.Join(dir, "c"), 3*cache.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		blk := make([]byte, cache.BlockSize)
		for j := range blk {
			blk[j] = byte(i)
		}
		if err := c.PutBlock("f", i, 0, blk, 256); err != nil {
			t.Fatal(err)
		}
	}
	total, chunks := c.Stats()
	if total > 3*cache.BlockSize {
		t.Fatalf("cache exceeded cap: %d bytes", total)
	}
	if chunks > 3 {
		t.Fatalf("expected at most 3 chunks after eviction, got %d", chunks)
	}
	// The most recent chunk must still be resident.
	if _, ok := c.GetBlock("f", 7, 0); !ok {
		t.Fatal("most recent chunk evicted")
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}