package table

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/polarsignals/iceberg-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
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

func (t *hdfsTable) SnapshotWriter() (SnapshotWriter, error) {
	return &hdfsSnapshotWriter{
		snapshotID: rand.Int63(),
		bucket:     t.bucket,
		version:    t.version,
		table:      t,
	}, nil
}

type hdfsSnapshotWriter struct {
	version    int
	snapshotID int64
	bucket     objstore.Bucket
	table      Table

	schema  *iceberg.Schema
	entries []iceberg.ManifestEntry
}

func (s *hdfsSnapshotWriter) metadataDir() string {
	return filepath.Join(s.table.Location(), metadataDirName)
}

func (s *hdfsSnapshotWriter) dataDir() string {
	return filepath.Join(s.table.Location(), dataDirName)
}

// TODO: Append operates in 'merge-schema' mode. Where it will automatically merge the schema of the new data with the existing schema.
// it might be worth setting this as an option.
func (s *hdfsSnapshotWriter) Append(ctx context.Context, r io.Reader) error {
	b := &bytes.Buffer{}
	rdr := io.TeeReader(r, b) // Read file into memory while uploading

	// TODO(thor): We may want to pass in the filename as an option.
	dataFile := filepath.Join(s.dataDir(), fmt.Sprintf("%s%s", generateULID(), parquetFileExt))
	if err := s.bucket.Upload(ctx, dataFile, rdr); err != nil {
		return err
	}

	// Create manifest entry
	entry, schema, err := iceberg.ManifestEntryV1FromParquet(dataFile, int64(b.Len()), bytes.NewReader(b.Bytes()))
	if err != nil {
		return err
	}

	// Merge the schema with the table schema
	if s.schema == nil {
		s.schema, err = s.table.Metadata().CurrentSchema().Merge(schema)
		if err != nil {
			return err
		}
	} else {
		s.schema, err = s.schema.Merge(schema)
		if err != nil {
			return err
		}
	}

	s.entries = append(s.entries, entry)
	return nil
}

func (s *hdfsSnapshotWriter) Close(ctx context.Context) error {

	// Upload the manifest file
	// TODO: This is fastAppend only. We also want to support a normal append; where the entry is appended to the previous manifest
	manifestFile := fmt.Sprintf("%s%s", generateULID(), manifestFileExt)
	path := filepath.Join(s.metadataDir(), manifestFile)
	if err := s.uploadManifest(ctx, path, func(ctx context.Context, w io.Writer) error {
		return iceberg.WriteManifestV1(w, s.entries)
	}); err != nil {
		return err
	}

	// Create manifest list
	// TODO: get the file size from the manifest file just written
	manifestList, err := s.createNewManifestList(path, 0, s.snapshotID, s.table)
	if err != nil {
		return err
	}

	// Upload the manifest list
	manifestListFile := fmt.Sprintf("snap-%v-%s%s", s.snapshotID, generateULID(), manifestFileExt)
	manifestListPath := filepath.Join(s.metadataDir(), manifestListFile)
	if err := s.uploadManifest(ctx, manifestListPath, func(ctx context.Context, w io.Writer) error {
		return iceberg.WriteManifestListV1(w, manifestList)
	}); err != nil {
		return err
	}

	// Create snapshot data
	snapshot := Snapshot{
		SnapshotID:     s.snapshotID,
		SequenceNumber: 0, // TODO(thor): Get the sequence number from the previous snapshot
		TimestampMs:    time.Now().UnixMilli(),
		ManifestList:   manifestListPath,
		Summary: &Summary{
			Operation: OpAppend,
		},
	}
	md, err := s.addSnapshot(ctx, s.table, snapshot, s.schema)
	if err != nil {
		return err
	}

	// Upload the metadata
	path = filepath.Join(s.metadataDir(), fmt.Sprintf("v%v.metadata%s", s.version+1, metadataFileExt))
	js, err := json.Marshal(md)
	if err != nil {
		return err
	}

	if err := s.bucket.Upload(ctx, path, bytes.NewReader(js)); err != nil {
		return err
	}

	// Upload the version hint
	hint := []byte(fmt.Sprintf("%v", s.version+1))
	path = filepath.Join(s.metadataDir(), hdfsVersionHintFile)
	if err := s.bucket.Upload(ctx, path, bytes.NewReader(hint)); err != nil {
		return err
	}

	return nil
}

func (s *hdfsSnapshotWriter) addSnapshot(ctx context.Context, t Table, snapshot Snapshot, schema *iceberg.Schema) (Metadata, error) {
	metadata := t.Metadata()

	if !t.Metadata().CurrentSchema().Equals(schema) {
		// need to only update the schema ID if it has changed
		schema.ID = metadata.CurrentSchema().ID + 1
	} else {
		schema.ID = metadata.CurrentSchema().ID
	}

	return NewMetadataV1Builder(
		metadata.Location(),
		schema,
		time.Now().UnixMilli(),
		schema.NumFields(),
	).
		WithTableUUID(metadata.TableUUID()).
		WithCurrentSchemaID(schema.ID).
		WithCurrentSnapshotID(snapshot.SnapshotID).
		WithSnapshots(append(metadata.Snapshots(), snapshot)).
		Build(), nil
}

// uploadManifest uploads a manifest to the iceberg table. It's a wrapper around the bucket upload which requires a io.Reader and the manifest write functions which requires a io.Writer.
func (s *hdfsSnapshotWriter) uploadManifest(ctx context.Context, path string, write func(ctx context.Context, w io.Writer) error) error {
	r, w := io.Pipe()

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		defer w.Close()
		return write(ctx, w)
	})

	errg.Go(func() error {
		return s.bucket.Upload(ctx, path, r)
	})

	return errg.Wait()
}

func (s *hdfsSnapshotWriter) createNewManifestList(manifestPath string, size, snapshotID int64, t Table) ([]iceberg.ManifestFile, error) {
	var previous []iceberg.ManifestFile
	snapshot := t.Metadata().CurrentSnapshot()
	if snapshot != nil {
		var err error
		previous, err = t.Metadata().CurrentSnapshot().Manifests(s.bucket)
		if err != nil {
			return nil, fmt.Errorf("failed to get previous manifests: %w", err)
		}
	}

	newmanifest := iceberg.NewManifestV1Builder(
		manifestPath,
		size,
		0,
		snapshotID,
	).
		AddedFiles(1). // TODO add other available fields
		Build()

	return append(previous, newmanifest), nil
}