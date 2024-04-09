package iceberg

import (
	"bytes"
	"fmt"
	"io"
	"text/template"
	"time"

	"github.com/hamba/avro/v2/ocf"
)

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

func WriteManifestV1(w io.Writer, entries []ManifestEntry) error {
	enc, err := ocf.NewEncoder(
		AvroSchemaFromEntriesV1(entries),
		w,
		ocf.WithMetadata(map[string][]byte{
			"format-version": []byte("1"),
			"schema":         []byte("todo"), // TODO
			"schema-id":      []byte("todo"), // TODO
			"partition-spec": []byte("todo"), // TODO
			"avro.codec":     []byte("deflate"),
		}),
		ocf.WithCodec(ocf.Deflate),
	)
	if err != nil {
		return err
	}
	defer enc.Close()

	for _, entry := range entries {
		if err := enc.Encode(entry); err != nil {
			return err
		}
	}

	return nil
}

// AvroSchemaFromEntriesV1 creates an Avro schema from the given manifest entries.
// The entries must all share the same partition spec.
func AvroSchemaFromEntriesV1(entries []ManifestEntry) string {
	partitions := entries[0].DataFile().Partition() // Pull the first entries partition spec since they are expected to be the same for all entries.
	b := &bytes.Buffer{}
	if err := template.Must(
		template.New("EntryV1Schema").
			Funcs(template.FuncMap{
				"Type": func(i any) string {
					switch t := i.(type) {
					case string:
						return `["null", "string"]`
					case int:
						return `["null", "int"]`
					case int64:
						return `["null", "long"]`
					case []byte:
						return `["null", "bytes"]`
					case time.Time:
						return `["null", {"type": "int", "logicalType": "date"}]`
					default:
						panic(fmt.Sprintf("unsupported type %T", t))
					}
				},
			}).
			Parse(EntryV1SchemaTmpl)).Execute(b, partitions); err != nil {
		panic(err)
	}

	return b.String()
}

// EntryV1SchemaTmpl is a Go text/template template for the Avro schema of a v1 manifest entry.
// It expects a map[string]any as the partitions as as the templated object. It calls a custom Type function to determine the Avro type for each partition value.
var EntryV1SchemaTmpl = `{
        "type": "record",
        "name": "manifest_entry",
        "fields": [
            {"name": "status", "type": "int", "field-id": 0},
            {"name": "snapshot_id", "type": "long", "field-id": 1},
            {
                "name": "data_file",
                "type": {
                    "type": "record",
                    "name": "r2",
                    "fields": [
                        {"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
                        {
                            "name": "file_format",
                            "type": "string",
                            "doc": "File format name: avro, orc, or parquet",
                            "field-id": 101
                        },
                        {{- if . }}
                        {
                            "name": "partition",
                            "type": {
                                "type": "record",
                                "name": "r102",
                                "fields": [
									{{ $first := true }}
                                    {{- range $key, $value := . }}
									{{ if $first }}
										{{ $first = false }}
									{{ else }}
										,
									{{ end }}
                                    {"name": "{{ $key }}", "type": {{ Type $value }}}
                                    {{- end }}
                                ]
                            },
                            "field-id": 102
                        },
                        {{- end }}
                        {"name": "record_count", "type": "long", "doc": "Number of records in the file", "field-id": 103},
                        {"name": "file_size_in_bytes", "type": "long", "doc": "Total file size in bytes", "field-id": 104},
                        {"name": "block_size_in_bytes", "type": "long", "field-id": 105},
                        {
                            "name": "column_sizes",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k117_v118",
                                        "fields": [
                                            {"name": "key", "type": "int", "field-id": 117},
                                            {"name": "value", "type": "long", "field-id": 118}
                                        ]
                                    },
                                    "logicalType": "map"
                                }
                            ],
                            "doc": "Map of column id to total size on disk",                            
                            "field-id": 108
                        },
                        {
                            "name": "value_counts",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k119_v120",
                                        "fields": [
                                            {"name": "key", "type": "int", "field-id": 119},
                                            {"name": "value", "type": "long", "field-id": 120}
                                        ]
                                    },
                                    "logicalType": "map"
                                }
                            ],
                            "doc": "Map of column id to total count, including null and NaN",                            
                            "field-id": 109
                        },
                        {
                            "name": "null_value_counts",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k121_v122",
                                        "fields": [
                                            {"name": "key", "type": "int", "field-id": 121},
                                            {"name": "value", "type": "long", "field-id": 122}
                                        ]
                                    },
                                    "logicalType": "map"
                                }
                            ],
                            "doc": "Map of column id to null value count",                            
                            "field-id": 110
                        },
                        {
                            "name": "nan_value_counts",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k138_v139",
                                        "fields": [
                                            {"name": "key", "type": "int", "field-id": 138},
                                            {"name": "value", "type": "long", "field-id": 139}
                                        ]
                                    },
                                    "logicalType": "map"
                                }
                            ],
                            "doc": "Map of column id to number of NaN values in the column",                            
                            "field-id": 137
                        },
                        {
                            "name": "lower_bounds",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k126_v127",
                                        "fields": [
                                            {"name": "key", "type": "int", "field-id": 126},
                                            {"name": "value", "type": "bytes", "field-id": 127}
                                        ]
                                    },
                                    "logicalType": "map"
                                }
                            ],
                            "doc": "Map of column id to lower bound",                            
                            "field-id": 125
                        },
                        {
                            "name": "upper_bounds",
                            "type": [
                                "null",
                                {
                                    "type": "array",
                                    "items": {
                                        "type": "record",
                                        "name": "k129_v130",
                                        "fields": [
                                            {"name": "key", "type": "int", "field-id": 129},
                                            {"name": "value", "type": "bytes", "field-id": 130}
                                        ]
                                    },
                                    "logicalType": "map"
                                }
                            ],
                            "doc": "Map of column id to upper bound",                            
                            "field-id": 128
                        },
                        {
                            "name": "key_metadata",
                            "type": ["null", "bytes"],
                            "doc": "Encryption key metadata blob",                            
                            "field-id": 131
                        },
                        {
                            "name": "split_offsets",
                            "type": ["null", {"type": "array", "items": "long", "element-id": 133}],
                            "doc": "Splittable offsets",                            
                            "field-id": 132
                        },
                        {
                            "name": "sort_order_id",
                            "type": ["null", "int"],
                            "doc": "Sort order ID",                            
                            "field-id": 140
                        }
                    ]
                },
                "field-id": 2
            }
        ]
    }`
