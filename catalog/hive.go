package catalog

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/thanos-io/objstore"
)

const (
	hiveTableMetadataDir = "metadata"
	hiveVersionHintFile  = "version-hint.text"
)

func hiveMetadataFileName(version int) string {
	return fmt.Sprintf("v%d.metadata.json", version)
}

type hive struct {
	bucket objstore.Bucket
}

func (h *hive) LoadTable(ctx context.Context, identifier table.Identifier, _ iceberg.Properties) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	// Load the latest version of the table.
	t, err := h.loadLatestTable(ctx, identifier, ns, tbl)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (h *hive) loadLatestTable(ctx context.Context, identifier table.Identifier, ns, tbl string) (*table.Table, error) {
	v, err := getTableVersion(ctx, h.bucket, ns, tbl)
	if err != nil {
		return nil, err
	}

	md, err := getTableMetadata(ctx, h.bucket, ns, tbl, v)
	if err != nil {
		return nil, err
	}

	// Scope the table bucket to the table's namespace and name.
	tableBucket := objstore.NewPrefixedBucket(h.bucket, filepath.Join(ns, tbl))
	return table.New(identifier, md, filepath.Join(ns, tbl, hiveTableMetadataDir, hiveMetadataFileName(md.Version())), tableBucket), nil
}

// getTableMetadata returns the metadata of the table at the specified version.
func getTableMetadata(ctx context.Context, bucket objstore.Bucket, ns, tbl string, version int) (table.Metadata, error) {
	r, err := bucket.Get(ctx, filepath.Join(ns, tbl, hiveTableMetadataDir, hiveMetadataFileName(version)))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata file: %w", err)
	}
	defer r.Close()

	md, err := table.ParseMetadata(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return md, nil
}

// getTableVersion returns the latest version of the table.
// FIXME: this could fallback to a version file scan instead of returning an error
func getTableVersion(ctx context.Context, bucket objstore.Bucket, ns, tbl string) (int, error) {
	r, err := bucket.Get(ctx, filepath.Join(ns, tbl, hiveTableMetadataDir, hiveVersionHintFile))
	if err != nil {
		return -1, fmt.Errorf("failed to get version hint file: %w", err)
	}
	defer r.Close()

	b, err := io.ReadAll(r)
	if err != nil {
		return -1, fmt.Errorf("failed to read version hint: %w", err)
	}

	v, err := strconv.Atoi(string(b))
	if err != nil {
		return -1, fmt.Errorf("failed to parse version hint: %w", err)
	}

	return v, nil
}
