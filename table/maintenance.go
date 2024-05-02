package table

import (
	"context"
	"path/filepath"
	"regexp"
	"time"

	"github.com/thanos-io/objstore"
)

var (
	metadataRegexp = regexp.MustCompile(`v[0-9]+\.metadata.json`)
	dataRegexp     = regexp.MustCompile(`[0-9A-Z]{26}.parquet`)
	snapshotRegexp = regexp.MustCompile(`snap-[0-9]{19}-[0-9A-Z]{26}\.avro`)
	manifestRegexp = regexp.MustCompile(`[0-9A-Z]{26}\.avro`)
	deletableFile  = []*regexp.Regexp{metadataRegexp, dataRegexp, snapshotRegexp, manifestRegexp}
)

// DeleteOrphanFiles will delete all files in the table's bucket that are not pointed to by any snapshot in the table.
// NOTE: https://iceberg.apache.org/docs/latest/maintenance/#delete-orphan-files
// "It is dangerous to remove orphan files with a retention interval shorter than the time expected for any write to complete because it might corrupt
// the table if in-progress files are considered orphaned and are deleted. The default interval is 3 days."
func DeleteOrphanFiles(ctx context.Context, table Table, age time.Duration) error {
	foundFiles := map[string]struct{}{
		filepath.Base(table.MetadataLocation()): {},
	}

	for _, logEntry := range table.Metadata().GetMetadataLog() {
		foundFiles[filepath.Base(logEntry.MetadataFile)] = struct{}{} // Record the metadata files that are still referenced
	}

	for _, snapshot := range table.Metadata().Snapshots() {
		foundFiles[filepath.Base(snapshot.ManifestList)] = struct{}{} // Record the manifest list file from each snapshot
		manifests, err := snapshot.Manifests(table.Bucket())
		if err != nil {
			return err
		}

		// Record the manifest file from each list
		for _, manifest := range manifests {
			foundFiles[filepath.Base(manifest.FilePath())] = struct{}{}

			// Record the data files from each manifest
			entries, _, err := manifest.FetchEntries(table.Bucket(), false)
			if err != nil {
				return err
			}

			for _, entry := range entries {
				foundFiles[filepath.Base(entry.DataFile().FilePath())] = struct{}{}
			}
		}
	}

	return table.Bucket().Iter(ctx, table.Location(), func(file string) error {
		if _, ok := foundFiles[filepath.Base(file)]; !ok {
			// If the file is not found in the list of referenced files and it's old enough AND it's a deletable file, delete it
			if !isDeletable(file) {
				return nil
			}
			info, err := table.Bucket().Attributes(ctx, file)
			if err != nil {
				return err
			}

			if time.Since(info.LastModified) < age {
				return nil
			}

			return table.Bucket().Delete(ctx, file)
		}
		return nil
	}, objstore.WithRecursiveIter)
}

func isDeletable(file string) bool {
	for _, reg := range deletableFile {
		if reg.MatchString(file) {
			return true
		}
	}
	return false
}
