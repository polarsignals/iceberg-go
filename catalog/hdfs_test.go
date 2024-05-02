package catalog

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func Test_HDFS(t *testing.T) {
	bucket := objstore.NewInMemBucket()

	catalog := NewHDFS("/", bucket)

	tablePath := filepath.Join("db", "test_table")

	ctx := context.Background()
	tbl, err := catalog.CreateTable(ctx, tablePath, iceberg.NewSchema(0), iceberg.Properties{})
	require.NoError(t, err)

	writer, err := tbl.SnapshotWriter(
		table.WithMergeSchema(),
	)
	require.NoError(t, err)

	type RowType struct{ FirstName, LastName string }

	b := &bytes.Buffer{}
	err = parquet.Write(b, []RowType{
		{FirstName: "Bob"},
		{FirstName: "Alice"},
	})
	require.NoError(t, err)

	require.NoError(t, writer.Append(ctx, b))

	require.NoError(t, writer.Close(ctx))

	// Read the data back
	tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	require.NoError(t, err)

	require.Equal(t, tbl.Location(), tablePath)
	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)

	manifests, err := snapshot.Manifests(bucket)
	require.NoError(t, err)

	require.Len(t, manifests, 1)
	entries, _, err := manifests[0].FetchEntries(bucket, false)
	require.NoError(t, err)

	require.Len(t, entries, 1)

	rc, err := bucket.Get(ctx, entries[0].DataFile().FilePath())
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })

	buf, err := io.ReadAll(rc)
	require.NoError(t, err)

	_, err = parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	t.Run("AppendManifestWithExpiration", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMergeSchema(),
			table.WithExpireSnapshotsOlderThan(time.Nanosecond),
			table.WithManifestSizeBytes(1024*1024),
		)
		require.NoError(t, err)

		prev := tbl.CurrentSnapshot().ManifestList
		_, err = bucket.Attributes(ctx, prev)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Charlie"},
			{FirstName: "David"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		// Expect where to only be one snapshot (the one we just added)
		require.Len(t, tbl.Metadata().Snapshots(), 1)

		// Expect there to be only one manifest file (it was appended to)
		mfst, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)
		require.Len(t, mfst, 1)

		// Expect the manifest file to have been deleted
		_, err = bucket.Attributes(ctx, prev)
		require.Error(t, err)
		require.True(t, bucket.IsObjNotFoundErr(err))
	})

	t.Run("FastAppendWithoutExpiration", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMergeSchema(),
			table.WithManifestSizeBytes(1024*1024),
			table.WithFastAppend(),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Batman"},
			{FirstName: "Robin"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		require.Len(t, tbl.Metadata().Snapshots(), 2)
		mfst, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)
		require.Len(t, mfst, 2)
	})

	t.Run("MergeSchema", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMergeSchema(),
			table.WithManifestSizeBytes(1024*1024),
			table.WithFastAppend(),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		type NewRowType struct{ FirstName, MiddleName, LastName string }
		err = parquet.Write(b, []NewRowType{
			{
				FirstName:  "Thomas",
				MiddleName: "Woodrow",
				LastName:   "Wilson",
			},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		require.Len(t, tbl.Metadata().Snapshots(), 3)
		mfst, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)
		require.Len(t, mfst, 3)

		require.Len(t, tbl.Metadata().CurrentSchema().Fields(), 3)
	})

	t.Run("RemoveStaleMetadata", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMetadataPreviousVersionsMax(1),
			table.WithMetadataDeleteAfterCommit(),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Steve"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		// Expect the metadata log to have been created.
		require.Len(t, tbl.Metadata().GetMetadataLog(), 1)
		file := tbl.Metadata().GetMetadataLog()[0].MetadataFile
		_, err = bucket.Attributes(ctx, file)
		require.NoError(t, err)

		w, err = tbl.SnapshotWriter(
			table.WithMetadataPreviousVersionsMax(1),
			table.WithMetadataDeleteAfterCommit(),
		)
		require.NoError(t, err)

		b = &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Erwin"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		require.Len(t, tbl.Metadata().GetMetadataLog(), 1)
		// Validate that the file was deleted
		_, err = bucket.Attributes(ctx, file)
		require.True(t, bucket.IsObjNotFoundErr(err))
	})

	t.Run("DeleteDataFiles", func(t *testing.T) {
		w, err := tbl.SnapshotWriter()
		require.NoError(t, err)

		// Just delete a single data file
		i := 0
		var deleted iceberg.DataFile
		w.DeleteDataFile(ctx, func(file iceberg.DataFile) bool {
			i++
			if i == 1 {
				deleted = file
			}
			return i == 1
		})

		require.NoError(t, w.Close(ctx))

		// Check to make sure the data is gone
		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		snap := tbl.CurrentSnapshot()
		manifests, err := snap.Manifests(bucket)
		require.NoError(t, err)

		for _, manifest := range manifests {
			entries, _, err := manifest.FetchEntries(bucket, false)
			require.NoError(t, err)

			for _, entry := range entries {
				require.NotEqual(t, deleted.FilePath(), entry.DataFile().FilePath())
			}
		}
	})

	t.Run("DeleteWithWrite", func(t *testing.T) {
		w, err := tbl.SnapshotWriter()
		require.NoError(t, err)

		// Just delete a single data file
		i := 0
		var deleted iceberg.DataFile
		w.DeleteDataFile(ctx, func(file iceberg.DataFile) bool {
			i++
			if i == 1 {
				deleted = file
			}
			return i == 1
		})

		// Write a new data file
		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Bartholomew"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))

		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		snap := tbl.CurrentSnapshot()
		manifests, err := snap.Manifests(bucket)
		require.NoError(t, err)

		found := false
		for _, manifest := range manifests {
			entries, _, err := manifest.FetchEntries(bucket, false)
			require.NoError(t, err)

			for _, entry := range entries {
				// Check to make sure the data is gone
				require.NotEqual(t, deleted.FilePath(), entry.DataFile().FilePath())
				// Check to make sure the new data is there
				if "Bartholomew" == string(entry.DataFile().LowerBoundValues()[0]) {
					found = true
				}
			}
		}

		require.True(t, found)
	})

	t.Run("DeleteWriteSameManifest", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithManifestSizeBytes(1024 * 1024),
		)
		require.NoError(t, err)

		manifests, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)

		latest := manifests[len(manifests)-1]
		latestEntries, _, err := latest.FetchEntries(bucket, false)
		require.NoError(t, err)

		// Pick a data file from the latest manifest to delete
		deleted := latestEntries[0].DataFile()
		w.DeleteDataFile(ctx, func(file iceberg.DataFile) bool {
			return file.FilePath() == deleted.FilePath()
		})

		// Write a new data file expecting it to be appended to the manifest we just deleted from
		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Scooby"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))

		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		snap := tbl.CurrentSnapshot()
		manifests, err = snap.Manifests(bucket)
		require.NoError(t, err)

		found := false
		for _, manifest := range manifests {
			entries, _, err := manifest.FetchEntries(bucket, false)
			require.NoError(t, err)

			for _, entry := range entries {
				// Check to make sure the data is gone
				require.NotEqual(t, deleted.FilePath(), entry.DataFile().FilePath())
				// Check to make sure the new data is there
				if "Scooby" == string(entry.DataFile().LowerBoundValues()[0]) {
					found = true
				}
			}
		}

		require.True(t, found)
	})

	t.Run("DeleteOrphanFiles", func(t *testing.T) {
		// Orphan a data file; expect the DeleteOrphanFiles function to remove it
		w, err := tbl.SnapshotWriter(
			table.WithManifestSizeBytes(1024*1024),
			table.WithExpireSnapshotsOlderThan(time.Nanosecond), // This will ensure no snapshots hold onto this data file and actually orphan it
		)
		require.NoError(t, err)
		i := 0
		deleted := ""
		w.DeleteDataFile(ctx, func(file iceberg.DataFile) bool {
			i++
			if i == 1 {
				deleted = file.FilePath()
			}
			return i == 1
		})
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)
		require.NoError(t, table.DeleteOrphanFiles(ctx, tbl, time.Nanosecond))

		require.NoError(t, tbl.Bucket().Iter(ctx, "", func(name string) error {
			require.NotEqual(t, deleted, name)
			return nil
		}, objstore.WithRecursiveIter))
	})
}
