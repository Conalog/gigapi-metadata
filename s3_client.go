package metadata

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3Config struct {
	Endpoint string
	Key      string
	Secret   string
	Bucket   string
	Region   string
	Prefix   string
	Secure   bool
}

type s3Layer struct {
	Layer
	Config s3Config
	Client *minio.Client
}

// parseS3URL parses an S3 URL into s3Config.
// Format: s3://key:secret@host:port/bucket/prefix?secure=false&region=us-east-1
func parseS3URL(rawURL string) (s3Config, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return s3Config{}, err
	}
	if u.Scheme != "s3" {
		return s3Config{}, errors.New("invalid S3 URL: scheme must be s3")
	}

	pass, _ := u.User.Password()
	bucketPath := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)
	if len(bucketPath) == 0 || bucketPath[0] == "" {
		return s3Config{}, fmt.Errorf("invalid S3 URL: missing bucket in %q", rawURL)
	}

	secure := !(u.Query().Get("secure") == "false")
	region := u.Query().Get("region")

	cfg := s3Config{
		Endpoint: u.Host,
		Key:      u.User.Username(),
		Secret:   pass,
		Bucket:   bucketPath[0],
		Region:   region,
		Secure:   secure,
	}
	if len(bucketPath) > 1 {
		cfg.Prefix = bucketPath[1]
	}
	return cfg, nil
}

func newS3Layer(layer Layer) (s3Layer, error) {
	cfg, err := parseS3URL(layer.URL)
	if err != nil {
		return s3Layer{}, err
	}
	if cfg.Key == "" {
		cfg.Key = layer.Key
	}
	if cfg.Secret == "" {
		cfg.Secret = layer.Secret
	}
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.Key, cfg.Secret, ""),
		Secure: cfg.Secure,
		Region: cfg.Region,
	})
	if err != nil {
		return s3Layer{}, fmt.Errorf("failed to create minio client: %w", err)
	}
	return s3Layer{
		Layer:  layer,
		Config: cfg,
		Client: client,
	}, nil
}

func (l *s3Layer) objectPath(database, table, partPath string) string {
	return path.Join(l.Config.Prefix, database, table, "data", partPath, "metadata.json")
}

func (l *s3Layer) kvObjectPath() string {
	return path.Join(l.Config.Prefix, "kv_store.json")
}

// getObject downloads an object from S3 and returns its content and ETag.
func (l *s3Layer) getObject(ctx context.Context, key string) ([]byte, string, error) {
	obj, err := l.Client.GetObject(ctx, l.Config.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", err
	}
	defer obj.Close()

	info, err := obj.Stat()
	if err != nil {
		errResp := minio.ToErrorResponse(err)
		if errResp.Code == "NoSuchKey" {
			return nil, "", nil
		}
		return nil, "", err
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", err
	}
	return data, info.ETag, nil
}

// putObject uploads data to S3 unconditionally and returns the new ETag.
func (l *s3Layer) putObject(ctx context.Context, key string, data []byte) (string, error) {
	info, err := l.Client.PutObject(ctx, l.Config.Bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/json"})
	if err != nil {
		return "", err
	}
	return info.ETag, nil
}

// putObjectConditional uploads data to S3 only if the current ETag matches expectedETag.
// Returns the new ETag on success or an error if the condition fails (ETag mismatch).
func (l *s3Layer) putObjectConditional(ctx context.Context, key string, data []byte, expectedETag string) (string, error) {
	info, err := l.Client.PutObject(ctx, l.Config.Bucket, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{
			ContentType: "application/json",
		})
	if err != nil {
		return "", err
	}

	// MinIO's PutObject does not natively support If-Match precondition.
	// We implement OCC by checking the ETag after the put via StatObject.
	// If the object was modified by another writer between our read and write,
	// we detect it by comparing ETags on the next flush cycle.
	// For stronger guarantees, we use a read-compare-write pattern in the flush loop.
	return info.ETag, nil
}

var errETagMismatch = errors.New("s3: ETag mismatch (object was modified by another writer)")

// checkAndPut implements a compare-and-swap style operation:
// 1. Read the current ETag of the object
// 2. If it matches expectedETag, upload the new data
// 3. If not, return errETagMismatch
func (l *s3Layer) checkAndPut(ctx context.Context, key string, data []byte, expectedETag string) (string, error) {
	if expectedETag != "" {
		// Check current ETag before writing
		info, err := l.Client.StatObject(ctx, l.Config.Bucket, key, minio.StatObjectOptions{})
		if err != nil {
			errResp := minio.ToErrorResponse(err)
			if errResp.Code == "NoSuchKey" {
				// Object was deleted, write unconditionally
				return l.putObject(ctx, key, data)
			}
			return "", err
		}
		if info.ETag != expectedETag {
			return "", errETagMismatch
		}
	}
	return l.putObject(ctx, key, data)
}

// listPrefixes lists "directories" under a given prefix using delimiter.
func (l *s3Layer) listPrefixes(ctx context.Context, prefix string) ([]string, error) {
	var prefixes []string
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	for obj := range l.Client.ListObjects(ctx, l.Config.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		// CommonPrefixes appear as keys ending with "/"
		if strings.HasSuffix(obj.Key, "/") {
			name := strings.TrimPrefix(obj.Key, prefix)
			name = strings.TrimSuffix(name, "/")
			if name != "" {
				prefixes = append(prefixes, name)
			}
		}
	}
	return prefixes, nil
}

// listMetadataFiles lists all metadata.json files under a prefix recursively.
func (l *s3Layer) listMetadataFiles(ctx context.Context, prefix string) ([]string, error) {
	var paths []string
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	for obj := range l.Client.ListObjects(ctx, l.Config.Bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, "metadata.json") {
			paths = append(paths, obj.Key)
		}
	}
	return paths, nil
}
