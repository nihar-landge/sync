package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

func New(baseURL, token string) *Client {
	return &Client{
		BaseURL: baseURL,
		Token:   token,
		HTTP:    &http.Client{Timeout: 60 * time.Second},
	}
}

type errorResponse struct {
	Detail any `json:"detail"`
}

func (c *Client) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var er errorResponse
		_ = json.Unmarshal(data, &er)
		return fmt.Errorf("%s %s: %s", method, path, errDetail(er, resp.StatusCode))
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

func errDetail(er errorResponse, code int) string {
	if er.Detail != nil {
		return fmt.Sprintf("%v", er.Detail)
	}
	return fmt.Sprintf("HTTP %d", code)
}

// ---------------------------------------------------------------------------

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type LoginResponse struct {
	ID           string `json:"id"`
	Username     string `json:"username"`
	AccessToken  string `json:"access_token"`
}

func (c *Client) Login(ctx context.Context, username, password string) error {
	var out LoginResponse
	err := c.do(ctx, "POST", "/api/v1/auth/login", LoginRequest{Username: username, Password: password}, &out)
	if err != nil {
		return err
	}
	c.Token = out.AccessToken
	return nil
}

type DirEntry struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Type  string `json:"type"`
	Size  int64  `json:"size"`
	Mtime string `json:"mtime"`
}

type ListResponse struct {
	Path    string     `json:"path"`
	Parent  string     `json:"parent"`
	Entries []DirEntry `json:"entries"`
}

func (c *Client) List(ctx context.Context, path string) (*ListResponse, error) {
	var out ListResponse
	err := c.do(ctx, "GET", "/api/v1/fs/list?path="+path, nil, &out)
	return &out, err
}

func (c *Client) Mkdir(ctx context.Context, path string) error {
	return c.do(ctx, "POST", "/api/v1/fs/mkdir", map[string]string{"path": path}, nil)
}

type InitUploadRequest struct {
	Path     string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
}

type InitUploadResponse struct {
	FileID     string `json:"file_id"`
	ChunkSize  int64  `json:"chunk_size"`
	ChunkCount int    `json:"chunk_count"`
	SizeBytes  int64  `json:"size_bytes"`
}

func (c *Client) InitUpload(ctx context.Context, path string, size int64) (*InitUploadResponse, error) {
	var out InitUploadResponse
	err := c.do(ctx, "POST", "/api/v1/files/init-upload", InitUploadRequest{Path: path, SizeBytes: size}, &out)
	return &out, err
}

type URLResponse struct {
	URL       string `json:"url"`
	ExpiresAt string `json:"expires_at"`
}

func (c *Client) UploadURL(ctx context.Context, fileID string, n int) (string, error) {
	var out URLResponse
	err := c.do(ctx, "GET", fmt.Sprintf("/api/v1/files/%s/chunks/%d/upload-url", fileID, n), nil, &out)
	return out.URL, err
}

func (c *Client) DownloadURL(ctx context.Context, fileID string, n int) (string, error) {
	var out URLResponse
	err := c.do(ctx, "GET", fmt.Sprintf("/api/v1/files/%s/chunks/%d/download-url", fileID, n), nil, &out)
	return out.URL, err
}

type ChunkCompleteRequest struct {
	Checksum  string `json:"checksum"`
	SizeBytes int64  `json:"size_bytes"`
}

func (c *Client) CompleteChunk(ctx context.Context, fileID string, n int, checksum string, size int64) error {
	return c.do(ctx, "POST", fmt.Sprintf("/api/v1/files/%s/chunks/%d/complete", fileID, n),
		ChunkCompleteRequest{Checksum: checksum, SizeBytes: size}, nil)
}

type ChunkMeta struct {
	Index     int    `json:"index"`
	ObjectKey string `json:"object_key"`
	SizeBytes int64  `json:"size_bytes"`
	Uploaded  bool   `json:"uploaded"`
	Checksum  string `json:"checksum"`
}

type FileMeta struct {
	ID         string      `json:"id"`
	Path       string      `json:"path"`
	SizeBytes  int64       `json:"size_bytes"`
	ChunkSize  int64       `json:"chunk_size"`
	ChunkCount int         `json:"chunk_count"`
	Status     string      `json:"status"`
	UpdatedAt  string      `json:"updated_at"`
	Chunks     []ChunkMeta `json:"chunks"`
}

func (c *Client) FileMeta(ctx context.Context, fileID string) (*FileMeta, error) {
	var out FileMeta
	err := c.do(ctx, "GET", fmt.Sprintf("/api/v1/files/%s/meta", fileID), nil, &out)
	return &out, err
}

func (c *Client) DeleteFile(ctx context.Context, fileID string) error {
	return c.do(ctx, "DELETE", fmt.Sprintf("/api/v1/files/%s", fileID), nil, nil)
}