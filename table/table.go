// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"reflect"
	"time"

	"github.com/oklog/ulid"
	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

const (
	manifestFileExt     = ".avro"
	metadataFileExt     = ".json"
	metadataDirName     = "metadata"
	dataDirName         = "data"
	hdfsVersionHintFile = "version-hint.text"
)

type Identifier = []string

type Table interface {
	Identifier() Identifier
	Metadata() Metadata
	MetadataLocation() string
	Bucket() objstore.Bucket
	Schema() *iceberg.Schema
	Spec() iceberg.PartitionSpec
	SortOrder() SortOrder
	Properties() iceberg.Properties
	Location() string
	CurrentSnapshot() *Snapshot
	SnapshotByID(id int64) *Snapshot
	SnapshotByName(name string) *Snapshot
	Schemas() map[int]*iceberg.Schema
	Equals(other Table) bool

	SnapshotWriter() (SnapshotWriter, error)
}

type ReadOnlyTable struct {
	*baseTable
}

func (r *ReadOnlyTable) SnapshotWriter() (SnapshotWriter, error) {
	return nil, fmt.Errorf("table is read-only")
}

type baseTable struct {
	identifier       Identifier
	metadata         Metadata
	metadataLocation string
	bucket           objstore.Bucket
}

func (t *baseTable) Equals(other Table) bool {
	return slices.Equal(t.identifier, other.Identifier()) &&
		t.metadataLocation == other.MetadataLocation() &&
		reflect.DeepEqual(t.metadata, other.Metadata())
}

func (t *baseTable) Identifier() Identifier   { return t.identifier }
func (t *baseTable) Metadata() Metadata       { return t.metadata }
func (t *baseTable) MetadataLocation() string { return t.metadataLocation }
func (t *baseTable) Bucket() objstore.Bucket  { return t.bucket }

func (t *baseTable) Schema() *iceberg.Schema              { return t.metadata.CurrentSchema() }
func (t *baseTable) Spec() iceberg.PartitionSpec          { return t.metadata.PartitionSpec() }
func (t *baseTable) SortOrder() SortOrder                 { return t.metadata.SortOrder() }
func (t *baseTable) Properties() iceberg.Properties       { return t.metadata.Properties() }
func (t *baseTable) Location() string                     { return t.metadata.Location() }
func (t *baseTable) CurrentSnapshot() *Snapshot           { return t.metadata.CurrentSnapshot() }
func (t *baseTable) SnapshotByID(id int64) *Snapshot      { return t.metadata.SnapshotByID(id) }
func (t *baseTable) SnapshotByName(name string) *Snapshot { return t.metadata.SnapshotByName(name) }
func (t *baseTable) Schemas() map[int]*iceberg.Schema {
	m := make(map[int]*iceberg.Schema)
	for _, s := range t.metadata.Schemas() {
		m[s.ID] = s
	}
	return m
}

func New(ident Identifier, meta Metadata, location string, bucket objstore.Bucket) Table {
	return &ReadOnlyTable{
		baseTable: &baseTable{
			identifier:       ident,
			metadata:         meta,
			metadataLocation: location,
			bucket:           bucket,
		},
	}
}

func NewFromLocation(ident Identifier, metalocation string, bucket objstore.Bucket) (Table, error) {
	var meta Metadata

	r, err := bucket.Get(context.Background(), metalocation)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if meta, err = ParseMetadata(r); err != nil {
		return nil, err
	}

	return New(ident, meta, metalocation, bucket), nil
}

// SnapshotWriter is an interface for writing a new snapshot to a table.
type SnapshotWriter interface {
	// Append accepts a ReaderAt object that should read the Parquet file that is to be added to the snapshot.
	Append(ctx context.Context, r io.ReaderAt, size int64) error

	// Close writes the new snapshot to the table and closes the writer. It is an error to call Append after Close.
	Close(ctx context.Context) error
}

type snapshotWriter struct {
	snapshotID int64
	bucket     objstore.Bucket
	table      Table

	entries []iceberg.ManifestEntry
}

func (s *snapshotWriter) metadataDir() string {
	return filepath.Join(s.table.Location(), metadataDirName)
}

func (s *snapshotWriter) dataDir() string {
	return filepath.Join(s.table.Location(), dataDirName)
}

func (s *snapshotWriter) Append(ctx context.Context, r io.Reader, size int64) error {
	b := &bytes.Buffer{}
	rdr := io.TeeReader(r, b) // Read file into memory while uploading

	if err := s.bucket.Upload(ctx, "todo", rdr); err != nil {
		return err
	}

	// Create manifest entry
	entry, err := iceberg.ManifestEntryV1FromParquet("todo", int64(b.Len()), bytes.NewReader(b.Bytes()))
	if err != nil {
		return err
	}

	s.entries = append(s.entries, entry)
	return nil
}

func (s *snapshotWriter) Close(ctx context.Context) error {

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
	if err := s.uploadManifest(ctx, filepath.Join(s.metadataDir(), manifestListFile), func(ctx context.Context, w io.Writer) error {
		return iceberg.WriteManifestListV1(w, manifestList)
	}); err != nil {
		return err
	}

	// Create snapshot data
	snapshot := Snapshot{
		SnapshotID:     s.snapshotID,
		SequenceNumber: 0, // TODO
		TimestampMs:    time.Now().UnixMilli(),
		ManifestList:   path,
		Summary: &Summary{
			Operation: OpAppend,
		},
	}
	md, err := s.addSnapshot(ctx, s.table, snapshot, nil) // TODO get schema from entries
	if err != nil {
		return err
	}

	// TODO: version metadata files are HDFS specific
	prevVersion := 0 // TODO: get the previous version from the metadata

	// Upload the metadata
	path = filepath.Join(s.metadataDir(), fmt.Sprintf("v%v.metadata%s", prevVersion+1, metadataFileExt))
	js, err := json.Marshal(md)
	if err != nil {
		return err
	}

	if err := s.bucket.Upload(ctx, path, bytes.NewReader(js)); err != nil {
		return err
	}

	// Update the version hint (TODO: HDFS only)
	hint := []byte(fmt.Sprintf("%v", prevVersion+1))
	path = filepath.Join(s.metadataDir(), hdfsVersionHintFile)
	if err := s.bucket.Upload(ctx, path, bytes.NewReader(hint)); err != nil {
		return err
	}

	return nil
}

func (s *snapshotWriter) addSnapshot(ctx context.Context, t Table, snapshot Snapshot, sc *parquet.Schema) (Metadata, error) {
	metadata := t.Metadata()

	// TODO need to only update the schema if it has changed
	schema := parquetSchemaToIcebergSchema(metadata.CurrentSchema().ID+1, sc)
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
func (s *snapshotWriter) uploadManifest(ctx context.Context, path string, write func(ctx context.Context, w io.Writer) error) error {
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

func (s *snapshotWriter) createNewManifestList(manifestPath string, size, snapshotID int64, t Table) ([]iceberg.ManifestFile, error) {
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

func generateULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}

func parquetSchemaToIcebergSchema(id int, schema *parquet.Schema) *iceberg.Schema {
	fields := make([]iceberg.NestedField, 0, len(schema.Fields()))
	for i, f := range schema.Fields() {
		fields = append(fields, iceberg.NestedField{
			Type:     parquetTypeToIcebergType(f.Type()),
			ID:       i,
			Name:     f.Name(),
			Required: f.Required(),
		})
	}
	return iceberg.NewSchema(id, fields...)
}

func parquetTypeToIcebergType(t parquet.Type) iceberg.Type {
	switch tp := t.Kind(); tp {
	case parquet.Boolean:
		return iceberg.BooleanType{}
	case parquet.Int32:
		return iceberg.Int32Type{}
	case parquet.Int64:
		return iceberg.Int64Type{}
	case parquet.Float:
		return iceberg.Float32Type{}
	case parquet.Double:
		return iceberg.Float64Type{}
	case parquet.ByteArray:
		return iceberg.BinaryType{}
	default:
		panic(fmt.Sprintf("unsupported parquet type: %v", tp))
	}
}
