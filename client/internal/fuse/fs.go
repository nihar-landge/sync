// Package fuse mounts the CloudStream virtual filesystem using go-fuse.
//
// Directory listings and attributes come from the control plane (via the
// in-memory metadata cache); file bytes are served by the chunk manager,
// which fetches blocks from Object Storage only when the kernel actually
// reads them. Writes go to the local write-back staging area and are
// uploaded on Flush/Release.
package fuse

import (
	"context"
	"sync"
	"syscall"
	"time"

	"cloudstream/internal/api"
	"cloudstream/internal/chunk"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// CloudFS is the root filesystem node.
type CloudFS struct {
	fs.Inode
	ctx context.Context
	m   *chunk.Manager

	mu      sync.Mutex
	dirTtl  time.Time          // when the in-memory listing cache expires
	entries map[string]*api.ListResponse // path -> listing (metadata cache)
	dirs    map[string]bool    // known directories (path set)
}

func Mount(ctx context.Context, m *chunk.Manager, mountpoint string) error {
	root := &CloudFS{
		ctx:     ctx,
		m:       m,
		entries: map[string]*api.ListResponse{},
		dirs:    map[string]bool{},
	}
	server, err := fs.Mount(mountpoint, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName: "cloudstream", Name: "cloudstream",
			Debug: false,
		},
	})
	if err != nil {
		return err
	}
	server.Wait()
	return nil
}

func (c *CloudFS) list(path string) (*api.ListResponse, error) {
	c.mu.Lock()
	if c.entries[path] != nil && time.Since(c.dirTtl) < 5*time.Second {
		resp := c.entries[path]
		c.mu.Unlock()
		return resp, nil
	}
	c.mu.Unlock()
	resp, err := c.m.API.List(c.ctx, path)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.entries[path] = resp
	c.dirTtl = time.Now()
	c.mu.Unlock()
	return resp, nil
}

func (c *CloudFS) childDir(path string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dirs[path]
}

func (c *CloudFS) markDir(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dirs[path] = true
	c.entries = map[string]*api.ListResponse{} // invalidate listings after mutation
	c.dirTtl = time.Time{}
}

// ---------------------------------------------------------------------------
// Directory node
// ---------------------------------------------------------------------------

type CloudDir struct {
	fs.Inode
	fsys *CloudFS
	path string
}

var _ = (fs.NodeLookuper)((*CloudDir)(nil))
var _ = (fs.NodeReaddirer)((*CloudDir)(nil))
var _ = (fs.NodeMkdirer)((*CloudDir)(nil))
var _ = (fs.NodeCreater)((*CloudDir)(nil))
var _ = (fs.NodeGetattrer)((*CloudDir)(nil))

func (d *CloudDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	resp, err := d.fsys.list(d.path)
	if err != nil {
		return nil, syscall.ENOENT
	}
	for _, e := range resp.Entries {
		if e.Name != name {
			continue
		}
		childPath := d.path
		if childPath != "/" {
			childPath += "/"
		}
		childPath += e.Name
		if e.Type == "dir" {
			child := &CloudDir{fsys: d.fsys, path: childPath}
			d.fsys.markDir(childPath)
			return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
		}
		f := &CloudFile{fsys: d.fsys, path: childPath}
		return d.NewInode(ctx, f, fs.StableAttr{Mode: syscall.S_IFREG, Ino: hashIno(childPath)}), 0
	}
	return nil, syscall.ENOENT
}

func (d *CloudDir) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	resp, err := d.fsys.list(d.path)
	if err != nil {
		return nil, syscall.EIO
	}
	var entries []fuse.DirEntry
	for _, e := range resp.Entries {
		typ := uint32(syscall.S_IFREG)
		if e.Type == "dir" {
			typ = uint32(syscall.S_IFDIR)
		}
		entries = append(entries, fuse.DirEntry{Name: e.Name, Mode: typ, Ino: hashIno(d.path + "/" + e.Name)})
	}
	return fs.NewListDirStream(entries), 0
}

func (d *CloudDir) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0o755
	return 0
}

func (d *CloudDir) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	path := d.path
	if path != "/" {
		path += "/"
	}
	path += name
	if err := d.fsys.m.API.Mkdir(ctx, path); err != nil {
		return nil, syscall.EIO
	}
	d.fsys.markDir(path)
	child := &CloudDir{fsys: d.fsys, path: path}
	return d.NewInode(ctx, child, fs.StableAttr{Mode: syscall.S_IFDIR}), 0
}

func (d *CloudDir) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	// Uploads of brand-new files go through `cloudstream put`; FUSE create
	// is deferred to M6 scope. Returning EROFS surfaces the limitation.
	return nil, nil, 0, syscall.EROFS
}

// ---------------------------------------------------------------------------
// File node (implemented in file.go)
// ---------------------------------------------------------------------------

func hashIno(path string) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range []byte(path) {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}