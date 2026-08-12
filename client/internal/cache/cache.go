// Package cache implements a disk-backed, byte-capped LRU chunk cache.
//
// A chunk (64 MB) is stored as a sparse container file plus a resident-block
// mask. Only blocks that were actually fetched over the network are resident,
// so a 10 MB dd out of a 20 GB file transfers ~10 MB, not 64 MB.
// LRU eviction works at chunk granularity (delete the whole sparse file).
package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const BlockSize = 256 * 1024 // 256 KiB granularity for range fetches
const BlockMaskName = ".mask.json"

var (
	Hits   atomic.Int64
	Misses atomic.Int64
)

type entry struct {
	key         string
	size        int64 // resident bytes
	lastAccess  int64 // unix nanos
	chunkBlocks int   // number of blocks in the full chunk
}

type Cache struct {
	dir     string
	maxSize int64
	mu      sync.Mutex
	lru     *lru.Cache[string, *entry]
	total   int64
}

func New(dir string, maxBytes int64) (*Cache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	maxEntries := int(maxBytes/(BlockSize*256)) + 16
	if maxEntries < 64 {
		maxEntries = 64
	}
	l, err := lru.New[string, *entry](maxEntries)
	if err != nil {
		return nil, err
	}
	c := &Cache{dir: dir, maxSize: maxBytes, lru: l}
	// Rebuild LRU index from disk on startup so eviction survives restarts.
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, BlockMaskName) {
			if e, ok := c.loadMask(path); ok {
				l.Add(e.key, e)
				c.total += e.size
			}
		}
		return nil
	})
	// Trim to bounds in case config shrank or disk was littered.
	for c.total > c.maxSize {
		c.evictLocked()
	}
	return c, nil
}

func (c *Cache) key(fileID string, chunk int) string {
	return fileID + ":" + fmt.Sprintf("%06d", chunk)
}

func (c *Cache) chunkDir(fileID string, chunk int) string {
	return filepath.Join(c.dir, fileID, fmt.Sprintf("chunk_%06d", chunk))
}

func (c *Cache) chunkFile(fileID string, chunk int) string {
	return filepath.Join(c.chunkDir(fileID, chunk), "data")
}

func (c *Cache) maskFile(fileID string, chunk int) string {
	return filepath.Join(c.chunkDir(fileID, chunk), BlockMaskName)
}

type maskFile struct {
	Blocks     int   `json:"blocks"`
	Resident   []bool `json:"resident"`
	ModifiedAt int64 `json:"modified_at"`
}

// loadMask reads a persisted chunk mask and returns the cache entry.
func (c *Cache) loadMask(path string) (*entry, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var mf maskFile
	if err := json.Unmarshal(b, &mf); err != nil || mf.Blocks <= 0 || len(mf.Resident) != mf.Blocks {
		return nil, false
	}
	n := 0
	for _, r := range mf.Resident {
		if r {
			n++
		}
	}
	rel, err := filepath.Rel(c.dir, path)
	if err != nil {
		return nil, false
	}
	parts := strings.Split(filepath.Dir(rel), string(filepath.Separator))
	if len(parts) != 2 {
		return nil, false
	}
	fileID := parts[0]
	idx := strings.TrimPrefix(parts[1], "chunk_")
	var chunk int
	if _, err := fmt.Sscanf(idx, "%d", &chunk); err != nil {
		return nil, false
	}
	return &entry{
		key:         c.key(fileID, chunk),
		size:        int64(n) * BlockSize,
		lastAccess:  mf.ModifiedAt,
		chunkBlocks: mf.Blocks,
	}, true
}

// mask returns the resident-block mask for a chunk, loading/creating as needed.
// Caller must hold c.mu when the entry is new.
func (c *Cache) mask(fileID string, chunk int, blocks int) ([]bool, error) {
	dir := c.chunkDir(fileID, chunk)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	mf := maskFile{Blocks: blocks, Resident: make([]bool, blocks), ModifiedAt: time.Now().UnixNano()}
	if b, err := os.ReadFile(c.maskFile(fileID, chunk)); err == nil {
		var loaded maskFile
		if json.Unmarshal(b, &loaded) == nil && loaded.Blocks == blocks {
			mf = loaded
			return mf.Resident, nil
		}
	}
	// Persist the (possibly fresh) mask immediately.
	if err := c.saveMaskLocked(fileID, chunk, mf.Resident, blocks); err != nil {
		return nil, err
	}
	return mf.Resident, nil
}

func (c *Cache) saveMaskLocked(fileID string, chunk int, resident []bool, blocks int) error {
	mf := maskFile{Blocks: blocks, Resident: resident, ModifiedAt: time.Now().UnixNano()}
	b, err := json.Marshal(mf)
	if err != nil {
		return err
	}
	if err := os.WriteFile(c.maskFile(fileID, chunk), b, 0o644); err != nil {
		return err
	}
	if e, ok := c.lru.Get(c.key(fileID, chunk)); ok {
		e.lastAccess = mf.ModifiedAt
	}
	return nil
}

// BlockData describes one resident block.
type BlockData struct {
	Index int
	Data  []byte
}

// GetBlock returns the given block's bytes, or (nil, false) if not resident.
func (c *Cache) GetBlock(fileID string, chunk, block int) ([]byte, bool) {
	key := c.key(fileID, chunk)
	c.mu.Lock()
	e, ok := c.lru.Get(key)
	if !ok {
		c.mu.Unlock()
		return nil, false
	}
	e.lastAccess = time.Now().UnixNano()
	c.mu.Unlock()

	f, err := os.Open(c.chunkFile(fileID, chunk))
	if err != nil {
		return nil, false
	}
	defer f.Close()
	buf := make([]byte, BlockSize)
	off := int64(block) * BlockSize
	if n, err := f.ReadAt(buf, off); err != nil || n != len(buf) {
		return nil, false
	}
	return buf, true
}

// PutBlock marks a block resident and writes its bytes to the sparse file.
func (c *Cache) PutBlock(fileID string, chunk, block int, data []byte, chunkBlocks int) error {
	if len(data) > BlockSize {
		return fmt.Errorf("block too large")
	}
	key := c.key(fileID, chunk)
	c.mu.Lock()
	defer c.mu.Unlock()

	e, ok := c.lru.Get(key)
	if !ok {
		var err error
		if e, err = c.newEntryLocked(key, fileID, chunk, chunkBlocks); err != nil {
			return err
		}
	}
	if block >= e.chunkBlocks {
		return fmt.Errorf("block %d out of range (%d blocks)", block, e.chunkBlocks)
	}
	e.lastAccess = time.Now().UnixNano()

	resident, err := c.mask(fileID, chunk, e.chunkBlocks)
	if err != nil {
		return err
	}

	if !resident[block] {
		dir := c.chunkDir(fileID, chunk)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(c.chunkFile(fileID, chunk), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		if _, err := f.WriteAt(data, int64(block)*BlockSize); err != nil {
			f.Close()
			return err
		}
		f.Close()
		resident[block] = true
		e.size += int64(len(data))
		c.total += int64(len(data))
		if err := c.saveMaskLocked(fileID, chunk, resident, e.chunkBlocks); err != nil {
			return err
		}
		for c.total > c.maxSize {
			c.evictLocked()
		}
	}
	return nil
}

func (c *Cache) newEntryLocked(key, fileID string, chunk, chunkBlocks int) (*entry, error) {
	e := &entry{key: key, chunkBlocks: chunkBlocks, lastAccess: time.Now().UnixNano()}
	c.lru.Add(key, e)
	return e, nil
}

func (c *Cache) evictLocked() {
	if c.lru.Len() == 0 {
		return
	}
	_, victim, ok := c.lru.RemoveOldest()
	if !ok {
		return
	}
	c.total -= victim.size
	parts := strings.Split(victim.key, ":")
	os.RemoveAll(c.chunkDir(parts[0], parseInt(parts[1])))
}

func parseInt(s string) int {
	var n int
	fmt.Sscanf(s, "%d", &n)
	return n
}

// DeleteFile drops every cached chunk belonging to a file.
func (c *Cache) DeleteFile(fileID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range c.lru.Keys() {
		parts := strings.Split(k, ":")
		if parts[0] == fileID {
			if v, ok := c.lru.Get(k); ok {
				c.total -= v.size
			}
			c.lru.Remove(k)
			os.RemoveAll(c.chunkDir(fileID, parseInt(parts[1])))
		}
	}
}

// Stats returns (resident bytes, chunk count) for reporting.
func (c *Cache) Stats() (int64, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.total, c.lru.Len()
}