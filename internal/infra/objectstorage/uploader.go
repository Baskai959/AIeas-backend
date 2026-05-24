package objectstorage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	appconfig "aieas_backend/internal/config"

	"github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	"github.com/volcengine/ve-tos-golang-sdk/v2/tos/enum"
)

var ErrDisabled = errors.New("object storage disabled")

type UploadInput struct {
	Filename    string
	ContentType string
	Size        int64
	Body        io.Reader
}

type Uploader interface {
	Upload(ctx context.Context, in UploadInput) (string, error)
}

type DisabledUploader struct{}

func (DisabledUploader) Upload(ctx context.Context, in UploadInput) (string, error) {
	_ = ctx
	_ = in
	return "", ErrDisabled
}

type TOSUploader struct {
	client       *tos.ClientV2
	bucket       string
	bucketURL    string
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
		bucketURL:    strings.TrimRight(strings.TrimSpace(cfg.BucketURL), "/"),
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
			ACL:           enum.ACLPublicRead,
		},
		Content: in.Body,
	})
	if err != nil {
		return "", err
	}
	return objectURL(u.bucketURL, key), nil
}

type MemoryUploader struct {
	baseURL string
	prefix  string
}

func NewMemoryUploader(baseURL string) *MemoryUploader {
	return &MemoryUploader{baseURL: strings.TrimRight(baseURL, "/")}
}

func (u *MemoryUploader) Upload(ctx context.Context, in UploadInput) (string, error) {
	_ = ctx
	if in.Body == nil {
		return "", fmt.Errorf("upload object: empty body")
	}
	if _, err := io.Copy(io.Discard, in.Body); err != nil {
		return "", err
	}
	key, err := buildObjectKey(u.prefix, in.Filename, in.ContentType)
	if err != nil {
		return "", err
	}
	return objectURL(u.baseURL, key), nil
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

func objectURL(baseURL, key string) string {
	if baseURL == "" {
		return key
	}
	escaped := make([]string, 0)
	for _, part := range strings.Split(key, "/") {
		if part == "" {
			continue
		}
		escaped = append(escaped, url.PathEscape(part))
	}
	return strings.TrimRight(baseURL, "/") + "/" + strings.Join(escaped, "/")
}

func normalizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	prefix = strings.Trim(prefix, "/")
	if prefix == "." || prefix == "" {
		return ""
	}
	return path.Clean(prefix)
}
