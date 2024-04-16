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
	"context"
	"reflect"

	"github.com/polarsignals/iceberg-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/exp/slices"
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
	return &baseTable{
		identifier:       ident,
		metadata:         meta,
		metadataLocation: location,
		bucket:           bucket,
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
