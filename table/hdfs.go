package table

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"github.com/thanos-io/objstore"
)

const (
	hdfsVersionHintFile = "version-hint.text"
	parquetFileExt      = ".parquet"
)

type hdfsTable struct {
	version int // The version of the table that has been loaded
	*baseTable
}

func NewHDFSTable(ver int, ident Identifier, meta Metadata, location string, bucket objstore.Bucket) Table {
	return &hdfsTable{
		version: ver,
		baseTable: &baseTable{
			identifier:       ident,
			metadata:         meta,
			metadataLocation: location,
			bucket:           bucket,
		},
	}
}

func (t *hdfsTable) SnapshotWriter(options ...WriterOption) (SnapshotWriter, error) {
	writer := &hdfsSnapshotWriter{}
	writer.snapshotWriter = NewSnapshotWriter(writer.commit, t.version, t.bucket, t, options...)
	return writer, nil
}

type hdfsSnapshotWriter struct {
	snapshotWriter
}

// commit is called by the underlying snapshotWriter to commit the snapshot.
// For HDFS, this means uploading the version hint file.
func (s *hdfsSnapshotWriter) commit(ctx context.Context, ver int) error {
	// Upload the version hint
	hint := []byte(fmt.Sprintf("%v", ver))
	path := filepath.Join(s.metadataDir(), hdfsVersionHintFile)
	return s.bucket.Upload(ctx, path, bytes.NewReader(hint))
}
