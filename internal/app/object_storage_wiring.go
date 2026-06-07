package app

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/url"
	"path/filepath"
	"strings"

	"aieas_backend/internal/domain"
	"aieas_backend/internal/infra/objectstorage"
	auctionports "aieas_backend/internal/modules/auction/ports"
	httptransport "aieas_backend/internal/transport/http"
)

type productAuditImageLoader struct {
	uploader objectstorage.Uploader
}

type objectStorageImageUploader struct {
	uploader objectstorage.Uploader
}

func (u objectStorageImageUploader) Upload(ctx context.Context, in httptransport.ImageUploadInput) (string, error) {
	if u.uploader == nil {
		return "", httptransport.ErrImageStorageDisabled
	}
	url, err := u.uploader.Upload(ctx, objectstorage.UploadInput{
		Filename:    in.Filename,
		ContentType: in.ContentType,
		Size:        in.Size,
		Body:        in.Body,
	})
	if err != nil {
		return "", mapImageStorageError(err)
	}
	return url, nil
}

func (u objectStorageImageUploader) Download(ctx context.Context, key string) (httptransport.ImageDownloadOutput, error) {
	if u.uploader == nil {
		return httptransport.ImageDownloadOutput{}, httptransport.ErrImageStorageDisabled
	}
	out, err := u.uploader.Download(ctx, key)
	if err != nil {
		return httptransport.ImageDownloadOutput{}, mapImageStorageError(err)
	}
	return httptransport.ImageDownloadOutput{
		Content:       out.Content,
		ContentType:   out.ContentType,
		ContentLength: out.ContentLength,
	}, nil
}

func mapImageStorageError(err error) error {
	switch {
	case errors.Is(err, objectstorage.ErrDisabled):
		return httptransport.ErrImageStorageDisabled
	case errors.Is(err, objectstorage.ErrInvalidObjectKey):
		return httptransport.ErrInvalidImageObjectKey
	case errors.Is(err, objectstorage.ErrObjectNotFound):
		return httptransport.ErrImageObjectNotFound
	default:
		return err
	}
}

func (l productAuditImageLoader) LoadProductAuditImage(ctx context.Context, imageURL string) (auctionports.ProductAuditImage, error) {
	if l.uploader == nil {
		return auctionports.ProductAuditImage{}, objectstorage.ErrDisabled
	}
	key, err := objectKeyFromProxyImageURL(imageURL)
	if err != nil {
		return auctionports.ProductAuditImage{}, err
	}
	out, err := l.uploader.Download(ctx, key)
	if err != nil {
		return auctionports.ProductAuditImage{}, err
	}
	defer out.Content.Close()
	image, err := io.ReadAll(out.Content)
	if err != nil {
		return auctionports.ProductAuditImage{}, err
	}
	contentType := strings.TrimSpace(out.ContentType)
	if contentType == "" {
		contentType = mime.TypeByExtension(strings.ToLower(filepath.Ext(key)))
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return auctionports.ProductAuditImage{
		ImageName:   filepath.Base(key),
		ContentType: contentType,
		ImageSize:   int64(len(image)),
		Image:       image,
	}, nil
}

func objectKeyFromProxyImageURL(imageURL string) (string, error) {
	value := strings.TrimSpace(imageURL)
	if value == "" {
		return "", domain.ErrInvalidArgument
	}
	if parsed, err := url.Parse(value); err == nil && parsed.Path != "" {
		value = parsed.Path
	}
	prefix := objectstorage.ProxyPathPrefix()
	if !strings.HasPrefix(value, prefix) {
		return "", domain.ErrInvalidArgument
	}
	key := strings.TrimSpace(strings.TrimPrefix(value, prefix))
	if key == "" || strings.Contains(key, "..") {
		return "", domain.ErrInvalidArgument
	}
	return key, nil
}
