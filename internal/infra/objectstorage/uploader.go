package objectstorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"path"
	"path/filepath"
	"strings"
	"sync"

	appconfig "aieas_backend/internal/config"

	"github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

var (
	ErrDisabled         = errors.New("object storage disabled")
	ErrInvalidObjectKey = errors.New("invalid object key")
	ErrObjectNotFound   = errors.New("object not found")
)

type UploadInput struct {
	Filename    string
	ContentType string
	Size        int64
	Body        io.Reader
}

type DownloadOutput struct {
	Content       io.ReadCloser
	ContentType   string
	ContentLength int64
}

type Uploader interface {
	Upload(ctx context.Context, in UploadInput) (string, error)
	Download(ctx context.Context, key string) (DownloadOutput, error)
}

type DisabledUploader struct{}

func (DisabledUploader) Upload(ctx context.Context, in UploadInput) (string, error) {
	_ = ctx
	_ = in
	return "", ErrDisabled
}

func (DisabledUploader) Download(ctx context.Context, key string) (DownloadOutput, error) {
	_ = ctx
	_ = key
	return DownloadOutput{}, ErrDisabled
}

type TOSUploader struct {
	client       *tos.ClientV2
	bucket       string
	objectPrefix string
}

func NewUploader(cfg appconfig.ObjectStorageConfig) (Uploader, error) {
	if !cfg.Enabled {
		return DisabledUploader{}, nil
	}
	client, err := tos.NewClientV2(
		strings.TrimSpace(cfg.Endpoint),
		tos.WithRegion(strings.TrimSpace(cfg.Region)),
		tos.WithCredentials(tos.NewStaticCredentials(strings.TrimSpace(cfg.AccessKey), strings.TrimSpace(cfg.SecretKey))),
	)
	if err != nil {
		return nil, err
	}
	return &TOSUploader{
		client:       client,
		bucket:       strings.TrimSpace(cfg.Bucket),
		objectPrefix: normalizePrefix(cfg.ObjectPrefix),
	}, nil
}

func (u *TOSUploader) Upload(ctx context.Context, in UploadInput) (string, error) {
	if in.Body == nil {
		return "", fmt.Errorf("upload object: empty body")
	}
	key, err := buildObjectKey(u.objectPrefix, in.Filename, in.ContentType)
	if err != nil {
		return "", err
	}
	_, err = u.client.PutObjectV2(ctx, &tos.PutObjectV2Input{
		PutObjectBasicInput: tos.PutObjectBasicInput{
			Bucket:        u.bucket,
			Key:           key,
			ContentLength: in.Size,
			ContentType:   strings.TrimSpace(in.ContentType),
		},
		Content: in.Body,
	})
	if err != nil {
		return "", err
	}
	return proxyURL(key), nil
}

func (u *TOSUploader) Download(ctx context.Context, key string) (DownloadOutput, error) {
	key, err := sanitizeObjectKey(key)
	if err != nil {
		return DownloadOutput{}, err
	}
	out, err := u.client.GetObjectV2(ctx, &tos.GetObjectV2Input{
		Bucket: u.bucket,
		Key:    key,
	})
	if err != nil {
		if tos.StatusCode(err) == 404 {
			return DownloadOutput{}, ErrObjectNotFound
		}
		return DownloadOutput{}, err
	}
	return DownloadOutput{
		Content:       out.Content,
		ContentType:   strings.TrimSpace(out.ContentType),
		ContentLength: out.ContentLength,
	}, nil
}

type MemoryUploader struct {
	prefix  string
	mu      sync.RWMutex
	objects map[string]memoryObject
}

type memoryObject struct {
	content     []byte
	contentType string
}

func NewMemoryUploader(baseURL string) *MemoryUploader {
	_ = baseURL
	return &MemoryUploader{objects: make(map[string]memoryObject)}
}

func (u *MemoryUploader) Upload(ctx context.Context, in UploadInput) (string, error) {
	_ = ctx
	if in.Body == nil {
		return "", fmt.Errorf("upload object: empty body")
	}
	content, err := io.ReadAll(in.Body)
	if err != nil {
		return "", err
	}
	key, err := buildObjectKey(u.prefix, in.Filename, in.ContentType)
	if err != nil {
		return "", err
	}
	u.mu.Lock()
	u.objects[key] = memoryObject{content: content, contentType: strings.TrimSpace(in.ContentType)}
	u.mu.Unlock()
	return proxyURL(key), nil
}

func (u *MemoryUploader) Download(ctx context.Context, key string) (DownloadOutput, error) {
	_ = ctx
	key, err := sanitizeObjectKey(key)
	if err != nil {
		return DownloadOutput{}, err
	}
	u.mu.RLock()
	obj, ok := u.objects[key]
	u.mu.RUnlock()
	if !ok {
		return DownloadOutput{}, ErrObjectNotFound
	}
	return DownloadOutput{
		Content:       io.NopCloser(bytes.NewReader(obj.content)),
		ContentType:   obj.contentType,
		ContentLength: int64(len(obj.content)),
	}, nil
}

func buildObjectKey(prefix, filename, contentType string) (string, error) {
	token, err := randomHex(16)
	if err != nil {
		return "", err
	}
	ext := strings.ToLower(filepath.Ext(filename))
	if ext == "" {
		ext = extensionByContentType(contentType)
	}
	name := token + ext
	if prefix == "" {
		return name, nil
	}
	return prefix + "/" + name, nil
}

func randomHex(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func extensionByContentType(contentType string) string {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return ""
	}
	exts, err := mime.ExtensionsByType(contentType)
	if err != nil || len(exts) == 0 {
		return ""
	}
	return exts[0]
}

func ProxyPathPrefix() string {
	return "/api/v1/images/"
}

func proxyURL(key string) string {
	return ProxyPathPrefix() + strings.TrimLeft(key, "/")
}

func sanitizeObjectKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	key = strings.TrimLeft(key, "/")
	key = path.Clean(key)
	if key == "." || key == "" || strings.HasPrefix(key, "../") || key == ".." {
		return "", ErrInvalidObjectKey
	}
	return key, nil
}

func normalizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.Trim(prefix, "/")
	if prefix == "." || prefix == "" {
		return ""
	}
	return path.Clean(prefix)
}
