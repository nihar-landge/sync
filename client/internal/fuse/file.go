package fuse

import (
	"context"
	"io"
	"sync"
	"syscall"

	"cloudstream/internal/cache"
	"cloudstream/internal/chunk"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type CloudFile struct {
	fs.Inode
	fsys *CloudFS
	path string

	mu     sync.Mutex
	handle *chunk.File
}

var _ = (fs.NodeGetattrer)((*CloudFile)(nil))
var _ = (fs.NodeOpener)((*CloudFile)(nil))
var _ = (fs.NodeReader)((*CloudFile)(nil))
var _ = (fs.NodeWriter)((*CloudFile)(nil))
var _ = (fs.NodeFlusher)((*CloudFile)(nil))

func (f *CloudFile) handleFor(ctx context.Context) (*chunk.File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.handle == nil {
		h, err := f.fsys.m.OpenPath(ctx, f.path)
		if err != nil {
			return nil, err
		}
		f.handle = h
	}
	return f.handle, nil
}

func (f *CloudFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	h, err := f.handleFor(ctx)
	if err != nil {
		return syscall.ENOENT
	}
	out.Mode = syscall.S_IFREG | 0o644
	out.Size = uint64(h.Meta.SizeBytes)
	out.Blksize = uint32(cache.BlockSize)
	return 0
}

func (f *CloudFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// A FileHandle is only needed for O_APPEND-style semantics; we support
	// positional reads/writes, so KEEP_CACHE is fine.
	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *CloudFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h, err := f.handleFor(ctx)
	if err != nil {
		return nil, syscall.ENOENT
	}
	n, err := h.ReadAt(ctx, dest, off)
	if n > 0 {
		return fuse.ReadResultData(dest[:n]), 0
	}
	if err == io.EOF {
		return fuse.ReadResultData(nil), 0
	}
	return nil, syscall.EIO
}

func (f *CloudFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	h, err := f.handleFor(ctx)
	if err != nil {
		return 0, syscall.ENOENT
	}
	n, err := h.WriteAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	return uint32(n), 0
}

func (f *CloudFile) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	f.mu.Lock()
	h := f.handle
	f.mu.Unlock()
	if h == nil {
		return 0
	}
	if err := h.Flush(ctx); err != nil {
		return syscall.EIO
	}
	return 0
}