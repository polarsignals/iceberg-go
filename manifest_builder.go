package iceberg

func NewManifestEntryV1(entryStatus ManifestEntryStatus, snapshotID int64, data dataFile) ManifestEntry {
	return &manifestEntryV1{
		EntryStatus: entryStatus,
		Snapshot:    snapshotID,
		Data:        data,
	}
}

type DataFileBuilder struct {
	*dataFile
}

func NewDataFileV1Builder(
	FilePath string,
	FileFormat FileFormat,
	PartitionSpec map[string]any,
	RecordCount int64,
	FileSizeBytes int64,
) DataFileBuilder {
	return DataFileBuilder{
		dataFile: &dataFile{
			Path:          FilePath,
			Format:        FileFormat,
			PartitionData: PartitionSpec,
			RecordCount:   RecordCount,
			FileSize:      FileSizeBytes,
		},
	}
}

func (builder DataFileBuilder) Build() DataFile {
	return builder.dataFile
}

func (builder DataFileBuilder) WithColumnSizes(columnSizes map[int]int64) DataFileBuilder {
	builder.ColSizes = avroColMapFromMap[int, int64](columnSizes)
	return builder
}

func (builder DataFileBuilder) WithVauecounts(valueCounts map[int]int64) DataFileBuilder {
	builder.ValCounts = avroColMapFromMap[int, int64](valueCounts)
	return builder
}

func (builder DataFileBuilder) WithNullValueCounts(nullValueCounts map[int]int64) DataFileBuilder {
	builder.NullCounts = avroColMapFromMap[int, int64](nullValueCounts)
	return builder
}

func (builder DataFileBuilder) WithNanValueCounts(nanValueCounts map[int]int64) DataFileBuilder {
	builder.NaNCounts = avroColMapFromMap[int, int64](nanValueCounts)
	return builder
}

func (builder DataFileBuilder) WithDistinctCounts(distinctCounts map[int]int64) DataFileBuilder {
	builder.DistinctCounts = avroColMapFromMap[int, int64](distinctCounts)
	return builder
}

func (builder DataFileBuilder) WithLowerBounds(lowerBounds map[int][]byte) DataFileBuilder {
	builder.LowerBounds = avroColMapFromMap[int, []byte](lowerBounds)
	return builder
}

func (builder DataFileBuilder) WithUpperBounds(upperBounds map[int][]byte) DataFileBuilder {
	builder.UpperBounds = avroColMapFromMap[int, []byte](upperBounds)
	return builder
}

func (builder DataFileBuilder) WithKeyMetadata(keyMetadata []byte) DataFileBuilder {
	builder.Key = &keyMetadata
	return builder
}

func (builder DataFileBuilder) WithSplitOffsets(splitOffsets []int64) DataFileBuilder {
	builder.Splits = &splitOffsets
	return builder
}

func (builder DataFileBuilder) WithSortOrderID(sortOrderID int) DataFileBuilder {
	builder.SortOrder = &sortOrderID
	return builder
}
