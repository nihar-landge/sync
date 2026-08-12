// Command cloudstream is the CloudStream client. It talks to the control
// plane for metadata and to Object Storage (via pre-authenticated URLs) for
// chunk data.
package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"cloudstream/internal/api"
	"cloudstream/internal/cache"
	"cloudstream/internal/chunk"
	"cloudstream/internal/fuse"
)

var (
	apiURL = flag.String("api", "", "control plane base URL (default: api_url in ~/.cloudstream/config.json)")
	user   = flag.String("user", "", "username (env CLOUDSTREAM_USER)")
	pass   = flag.String("pass", "", "password (env CLOUDSTREAM_PASS)")
)

type config struct {
	APIURL     string `json:"api_url"`
	Token      string `json:"token"`
	CacheSizeMB int64  `json:"cache_size_mb"`
}

const defaultCacheMB = 5120

func configPath() string {
	return filepath.Join(os.Getenv("HOME"), ".cloudstream", "config.json")
}

func loadConfig() (*config, error) {
	b, err := os.ReadFile(configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return &config{CacheSizeMB: defaultCacheMB}, nil
		}
		return nil, err
	}
	var c config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	if c.CacheSizeMB == 0 {
		c.CacheSizeMB = defaultCacheMB
	}
	return &c, nil
}

func saveConfig(c *config) error {
	if err := os.MkdirAll(filepath.Dir(configPath()), 0o755); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(c, "", "  ")
	return os.WriteFile(configPath(), b, 0o600)
}

func extractFlags(args []string) (api, user, pass string) {
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--api" && i+1 < len(args):
			api, i = args[i+1], i+1
		case strings.HasPrefix(args[i], "--api="):
			api = strings.TrimPrefix(args[i], "--api=")
		case args[i] == "--user" && i+1 < len(args):
			user, i = args[i+1], i+1
		case strings.HasPrefix(args[i], "--user="):
			user = strings.TrimPrefix(args[i], "--user=")
		case args[i] == "--pass" && i+1 < len(args):
			pass, i = args[i+1], i+1
		case strings.HasPrefix(args[i], "--pass="):
			pass = strings.TrimPrefix(args[i], "--pass=")
		}
	}
	return
}

func credentials() (string, string) {
	u, p := *user, *pass
	if u == "" {
		u = os.Getenv("CLOUDSTREAM_USER")
	}
	if p == "" {
		p = os.Getenv("CLOUDSTREAM_PASS")
	}
	return u, p
}

func (c *config) baseURL() string {
	if *apiURL != "" {
		return strings.TrimSuffix(*apiURL, "/")
	}
	return strings.TrimSuffix(c.APIURL, "/")
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `usage: cloudstream <command> [flags]

commands:
  login    store an API token (needs --api, --user, --pass)
  ls       list a virtual directory
  mkdir    create a virtual directory
  put      upload a local file to a virtual path
  get      download a virtual file to local disk
  cat      stream a virtual file to stdout
  rm       delete a virtual file
  stat     show metadata for a virtual file
  mount    FUSE-mount a virtual directory (requires macFUSE / fuse3)
  cache    show cache statistics
`)
		flag.PrintDefaults()
		os.Exit(2)
	}
	flag.Parse()
	// The stdlib flag package stops at the first positional arg, so
	// `cloudstream login --user foo` would miss --user. Pre-scan the raw
	// args for our flags regardless of position.
	apiVal, userVal, passVal := extractFlags(os.Args[1:])
	if apiVal != "" {
		*apiURL = apiVal
	}
	if userVal != "" {
		*user = userVal
	}
	if passVal != "" {
		*pass = passVal
	}
	if flag.NArg() < 1 {
		flag.Usage()
	}
	c, err := loadConfig()
	if err != nil {
		fatal(err)
	}
	ctx := context.Background()
	switch flag.Arg(0) {
	case "login":
		cmdLogin(ctx, c)
	case "ls":
		cmdLs(ctx, c, flag.Arg(1))
	case "mkdir":
		cmdMkdir(ctx, c, flag.Arg(1))
	case "put":
		cmdPut(ctx, c, flag.Arg(1), flag.Arg(2))
	case "get":
		cmdGet(ctx, c, flag.Arg(1), flag.Arg(2))
	case "cat":
		cmdCat(ctx, c, flag.Arg(1))
	case "rm":
		cmdRm(ctx, c, flag.Arg(1))
	case "stat":
		cmdStat(ctx, c, flag.Arg(1))
	case "mount":
		cmdMount(ctx, c, flag.Arg(1))
	case "cache":
		cmdCache(c)
	default:
		flag.Usage()
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "cloudstream:", err)
	os.Exit(1)
}

func managerFor(c *config) (*chunk.Manager, error) {
	base := c.baseURL()
	if base == "" {
		return nil, fmt.Errorf("no API URL: set --api or api_url in ~/.cloudstream/config.json")
	}
	apic := api.New(base, c.Token)
	if apic.Token == "" {
		return nil, fmt.Errorf("not logged in: run `cloudstream login`")
	}
	cc, err := cache.New(filepath.Join(os.Getenv("HOME"), ".cloudstream", "cache"), c.CacheSizeMB*1024*1024)
	if err != nil {
		return nil, err
	}
	return chunk.NewManager(apic, cc, filepath.Join(os.Getenv("HOME"), ".cloudstream", "staging")), nil
}

// ---------------------------------------------------------------------------

func cmdLogin(ctx context.Context, c *config) {
	u, p := credentials()
	if u == "" || p == "" {
		fatal(fmt.Errorf("login needs --user and --pass (or CLOUDSTREAM_USER/CLOUDSTREAM_PASS)"))
	}
	base := c.baseURL()
	if base == "" {
		fatal(fmt.Errorf("login needs --api"))
	}
	apic := api.New(base, "")
	if err := apic.Login(ctx, u, p); err != nil {
		fatal(err)
	}
	c.Token = apic.Token
	c.APIURL = base
	if err := saveConfig(c); err != nil {
		fatal(err)
	}
	fmt.Println("logged in")
}

func cmdLs(ctx context.Context, c *config, path string) {
	if path == "" {
		path = "/"
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	list, err := m.API.List(ctx, path)
	if err != nil {
		fatal(err)
	}
	if len(list.Entries) == 0 {
		fmt.Println("(empty)")
		return
	}
	for _, e := range list.Entries {
		if e.Type == "dir" {
			fmt.Printf("drwxr-xr-x          - %s/\n", e.Name)
		} else {
			fmt.Printf("-rw-r--r-- %12d %s\n", e.Size, e.Name)
		}
	}
}

func cmdMkdir(ctx context.Context, c *config, path string) {
	if path == "" {
		fatal(fmt.Errorf("usage: mkdir <path>"))
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	if err := m.API.Mkdir(ctx, path); err != nil {
		fatal(err)
	}
	fmt.Println("ok")
}

func cmdPut(ctx context.Context, c *config, local, remote string) {
	if local == "" || remote == "" {
		fatal(fmt.Errorf("usage: put <local> <remote>"))
	}
	info, err := os.Stat(local)
	if err != nil {
		fatal(err)
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	init, err := m.API.InitUpload(ctx, remote, info.Size())
	if err != nil {
		fatal(err)
	}
	src, err := os.Open(local)
	if err != nil {
		fatal(err)
	}
	defer src.Close()

	fmt.Printf("uploading %s -> %s (%d chunks)\n", local, remote, init.ChunkCount)
	buf := make([]byte, init.ChunkSize)
	for i := 0; i < init.ChunkCount; i++ {
		n, err := io.ReadFull(src, buf)
		if err != nil && err != io.ErrUnexpectedEOF {
			fatal(err)
		}
		if n == 0 {
			break
		}
		putChunk(ctx, m, init.FileID, i, buf[:n])
		fmt.Printf("\r  chunk %d/%d", i+1, init.ChunkCount)
	}
	fmt.Println("\ndone")
}

func putChunk(ctx context.Context, m *chunk.Manager, fileID string, idx int, data []byte) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		url, err := m.API.UploadURL(ctx, fileID, idx)
		if err != nil {
			lastErr = err
			continue
		}
		if err := putBytes(ctx, url, data); err != nil {
			lastErr = err
			continue
		}
		sum := md5hex(data)
		if err := m.API.CompleteChunk(ctx, fileID, idx, sum, int64(len(data))); err != nil {
			fatal(err)
		}
		return
	}
	fatal(fmt.Errorf("chunk %d upload failed: %v", idx, lastErr))
}

func cmdGet(ctx context.Context, c *config, remote, local string) {
	if remote == "" || local == "" {
		fatal(fmt.Errorf("usage: get <remote> <local>"))
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	f, err := m.OpenPath(ctx, remote)
	if err != nil {
		fatal(err)
	}
	dst, err := os.Create(local)
	if err != nil {
		fatal(err)
	}
	defer dst.Close()
	if err := streamTo(ctx, f, dst); err != nil {
		fatal(err)
	}
	fmt.Printf("downloaded %s to %s\n", remote, local)
}

func cmdCat(ctx context.Context, c *config, remote string) {
	if remote == "" {
		fatal(fmt.Errorf("usage: cat <remote>"))
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	f, err := m.OpenPath(ctx, remote)
	if err != nil {
		fatal(err)
	}
	if err := streamTo(ctx, f, os.Stdout); err != nil {
		fatal(err)
	}
}

func streamTo(ctx context.Context, f *chunk.File, w io.Writer) error {
	buf := make([]byte, 1<<20)
	off := int64(0)
	for {
		n, err := f.ReadAt(ctx, buf, off)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		off += int64(n)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("read stalled at offset %d", off)
		}
	}
}

func cmdRm(ctx context.Context, c *config, path string) {
	if path == "" {
		fatal(fmt.Errorf("usage: rm <path>"))
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	meta, err := m.Resolve(ctx, path)
	if err != nil {
		fatal(err)
	}
	if err := m.API.DeleteFile(ctx, meta.ID); err != nil {
		fatal(err)
	}
	m.Cache.DeleteFile(meta.ID)
	fmt.Println("deleted")
}

func cmdStat(ctx context.Context, c *config, path string) {
	if path == "" {
		fatal(fmt.Errorf("usage: stat <path>"))
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	meta, err := m.Resolve(ctx, path)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("path:   %s\n", meta.Path)
	fmt.Printf("id:     %s\n", meta.ID)
	fmt.Printf("size:   %d bytes (%.2f GiB)\n", meta.SizeBytes, float64(meta.SizeBytes)/(1024*1024*1024))
	fmt.Printf("chunks: %d x %d bytes\n", meta.ChunkCount, meta.ChunkSize)
	fmt.Printf("status: %s\n", meta.Status)
}

func cmdCache(c *config) {
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	bytes, chunks := m.Cache.Stats()
	fmt.Printf("cache: %d chunks, %.2f MiB resident (hits=%d misses=%d)\n",
		chunks, float64(bytes)/(1024*1024), cache.Hits.Load(), cache.Misses.Load())
}

func cmdMount(ctx context.Context, c *config, mountpoint string) {
	if mountpoint == "" {
		fatal(fmt.Errorf("usage: mount <mountpoint>"))
	}
	m, err := managerFor(c)
	if err != nil {
		fatal(err)
	}
	if err := fuse.Mount(ctx, m, mountpoint); err != nil {
		fatal(err)
	}
}

func putBytes(ctx context.Context, url string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: HTTP %d", url, resp.StatusCode)
	}
	return nil
}

func md5hex(b []byte) string {
	h := md5.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}