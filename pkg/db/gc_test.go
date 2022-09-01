/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package db

import (
	"context"
	"io"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/containerd/containerd/gc"
	"github.com/containerd/containerd/metadata/boltutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

func TestResourceMax(t *testing.T) {
	if ResourceContent != resourceContentFlat&gc.ResourceMax {
		t.Fatalf("Invalid flat content type: %d (max %d)", resourceContentFlat, gc.ResourceMax)
	}
	if ResourceSnapshot != resourceSnapshotFlat&gc.ResourceMax {
		t.Fatalf("Invalid flat snapshot type: %d (max %d)", resourceSnapshotFlat, gc.ResourceMax)
	}
}

func TestGCRoots(t *testing.T) {
	db, err := newDatabase(t)
	require.NoError(t, err)

	alters := []alterFunc{
		addImage("image1", dgst(1), nil),
		//addImage("image2", dgst(2), labelmap(string(labelGCSnapRef)+"overlay", "sn2")),
		addImage("image3", dgst(10), labelmap(string(labelGCContentRef), dgst(11).String())),
		/*
			addContainer("container1", "overlay", "sn4", nil),
			addContainer("container2", "overlay", "sn5", labelmap(string(labelGCSnapRef)+"overlay", "sn6")),
			addContainer("container3", "overlay", "sn7", labelmap(
				string(labelGCSnapRef)+"overlay/anything-1", "sn8",
				string(labelGCSnapRef)+"overlay/anything-2", "sn9",
				string(labelGCContentRef), dgst(7).String())),
			addContainer("container4", "", "", labelmap(
				string(labelGCContentRef)+".0", dgst(8).String(),
				string(labelGCContentRef)+".1", dgst(9).String())),
		*/
		addContent(dgst(1), nil),
		addContent(dgst(2), nil),
		addContent(dgst(3), nil),
		addContent(dgst(7), nil),
		addContent(dgst(8), nil),
		addContent(dgst(9), nil),
		addContent(dgst(12), nil),
		addContent(dgst(13), labelmap(string(labelGCRoot), "always")),
		addIngest("ingest-1", "", nil),       // will be seen as expired
		addIngest("ingest-2", "", timeIn(0)), // expired
		addIngest("ingest-3", "", timeIn(time.Hour)),
		addIngest("ingest-4", "", nil),
		addIngest("ingest-5", dgst(8), nil),
		addIngest("ingest-6", "", nil),      // added to expired lease
		addIngest("ingest-7", dgst(9), nil), // added to expired lease
		//addSnapshot("overlay", "sn1", "", nil),
		//addSnapshot("overlay", "sn2", "", nil),
		//addSnapshot("overlay", "sn3", "", labelmap(string(labelGCRoot), "always")),
		//addSnapshot("overlay", "sn4", "", nil),
		//addSnapshot("overlay", "sn5", "", nil),
		//addSnapshot("overlay", "sn6", "", nil),
		//addSnapshot("overlay", "sn7", "", nil),
		//addSnapshot("overlay", "sn8", "", nil),
		//addSnapshot("overlay", "sn9", "", nil),
		//addLeaseSnapshot("l1", "overlay", "sn5"),
		//addLeaseSnapshot("l2", "overlay", "sn6"),
		addLeaseContent("l1", dgst(4)),
		addLeaseContent("l2", dgst(5)),
		addLease("l3", labelmap(string(labelGCExpire), time.Now().Add(time.Hour).Format(time.RFC3339))),
		addLeaseContent("l3", dgst(6)),
		//addLeaseSnapshot("l3", "overlay", "sn7"),
		addLeaseIngest("l3", "ingest-4"),
		addLeaseIngest("l3", "ingest-5"),
		addLease("l4", labelmap(string(labelGCExpire), time.Now().Format(time.RFC3339))),
		addLeaseContent("l4", dgst(7)),
		//addLeaseSnapshot("l4", "overlay", "sn8"),
		addLeaseIngest("l4", "ingest-6"),
		addLeaseIngest("l4", "ingest-7"),

		addLease("l5", labelmap(string(labelGCFlat), time.Now().Add(time.Hour).Format(time.RFC3339))),
		addLeaseContent("l5", dgst(12)),
		//addLeaseSnapshot("l5", "overlay", "sn1"),
		addLeaseIngest("l5", "ingest-8"),
	}

	expected := []gc.Node{
		gcnode(ResourceContent, dgst(1).String()),
		gcnode(ResourceContent, dgst(4).String()),
		gcnode(ResourceContent, dgst(5).String()),
		gcnode(ResourceContent, dgst(6).String()),
		gcnode(ResourceContent, dgst(10).String()),
		gcnode(ResourceContent, dgst(11).String()),
		gcnode(ResourceContent, dgst(13).String()),
		/*
			gcnode(ResourceSnapshot, "overlay/sn2"),
			gcnode(ResourceSnapshot, "overlay/sn3"),
			gcnode(ResourceSnapshot, "overlay/sn4"),
			gcnode(ResourceSnapshot, "overlay/sn5"),
			gcnode(ResourceSnapshot, "overlay/sn6"),
			gcnode(ResourceSnapshot, "overlay/sn7"),
			gcnode(ResourceSnapshot, "overlay/sn8"),
			gcnode(ResourceSnapshot, "overlay/sn9"),
			gcnode(ResourceSnapshot, "overlay/sn5"),
			gcnode(ResourceSnapshot, "overlay/sn6"),
			gcnode(ResourceSnapshot, "overlay/sn7"),
		*/
		gcnode(ResourceLease, "l1"),
		gcnode(ResourceLease, "l2"),
		gcnode(ResourceLease, "l3"),
		gcnode(ResourceIngest, "ingest-3"),
		gcnode(ResourceIngest, "ingest-4"),
		gcnode(ResourceIngest, "ingest-5"),
		gcnode(ResourceLease, "l5"),
		gcnode(ResourceIngest, "ingest-8"),
		gcnode(resourceContentFlat, dgst(12).String()),
		//gcnode(resourceSnapshotFlat, "overlay/sn1"),
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		v1bkt, err := tx.CreateBucketIfNotExists(bucketKeyVersion)
		if err != nil {
			return err
		}
		for _, alter := range alters {
			if err := alter(v1bkt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update failed: %+v", err)
	}

	ctx := context.Background()

	checkNodeC(ctx, t, db, expected, func(ctx context.Context, tx *bolt.Tx, nc chan<- gc.Node) error {
		return startGCContext(ctx, nil).scanRoots(ctx, tx, nc)
	})
}

func TestGCRemove(t *testing.T) {
	db, err := newDatabase(t)
	require.NoError(t, err)

	alters := []alterFunc{
		addImage("image1", dgst(1), nil),
		//addImage("image2", dgst(2), labelmap(string(labelGCSnapRef)+"overlay", "sn2")),
		//addContainer("container1", "overlay", "sn4", nil),
		addContent(dgst(1), nil),
		addContent(dgst(2), nil),
		addContent(dgst(3), nil),
		addContent(dgst(4), nil),
		addContent(dgst(5), labelmap(string(labelGCRoot), "always")),
		addIngest("ingest-1", "", nil),
		addIngest("ingest-2", "", timeIn(0)),
		/*
			addSnapshot("overlay", "sn1", "", nil),
			addSnapshot("overlay", "sn2", "", nil),
			addSnapshot("overlay", "sn3", "", labelmap(string(labelGCRoot), "always")),
			addSnapshot("overlay", "sn4", "", nil),
			addSnapshot("overlay", "sn1", "", nil),
		*/
		addLease("l1", labelmap(string(labelGCExpire), time.Now().Add(time.Hour).Format(time.RFC3339))),
		addLease("l2", labelmap(string(labelGCExpire), time.Now().Format(time.RFC3339))),
	}

	all := []gc.Node{
		gcnode(ResourceContent, dgst(1).String()),
		gcnode(ResourceContent, dgst(2).String()),
		gcnode(ResourceContent, dgst(3).String()),
		gcnode(ResourceContent, dgst(4).String()),
		gcnode(ResourceContent, dgst(5).String()),
		/*
			gcnode(ResourceSnapshot, "overlay/sn1"),
			gcnode(ResourceSnapshot, "overlay/sn2"),
			gcnode(ResourceSnapshot, "overlay/sn3"),
			gcnode(ResourceSnapshot, "overlay/sn4"),
			gcnode(ResourceSnapshot, "overlay/sn1"),
		*/
		gcnode(ResourceLease, "l1"),
		gcnode(ResourceLease, "l2"),
		gcnode(ResourceIngest, "ingest-1"),
		gcnode(ResourceIngest, "ingest-2"),
	}

	var deleted, remaining []gc.Node
	for i, n := range all {
		if i%2 == 0 {
			deleted = append(deleted, n)
		} else {
			remaining = append(remaining, n)
		}
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		v1bkt, err := tx.CreateBucketIfNotExists(bucketKeyVersion)
		if err != nil {
			return err
		}
		for _, alter := range alters {
			if err := alter(v1bkt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update failed: %+v", err)
	}

	ctx := context.Background()
	c := startGCContext(ctx, nil)

	checkNodes(ctx, t, db, all, func(ctx context.Context, tx *bolt.Tx, fn func(context.Context, gc.Node) error) error {
		return c.scanAll(ctx, tx, fn)
	})
	if t.Failed() {
		t.Fatal("Scan all failed")
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, n := range deleted {
			if err := c.remove(ctx, tx, n); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update failed: %+v", err)
	}

	checkNodes(ctx, t, db, remaining, func(ctx context.Context, tx *bolt.Tx, fn func(context.Context, gc.Node) error) error {
		return c.scanAll(ctx, tx, fn)
	})
}

func TestGCRefs(t *testing.T) {
	db, err := newDatabase(t)
	require.NoError(t, err)

	alters := []alterFunc{
		addContent(dgst(1), nil),
		addContent(dgst(2), nil),
		addContent(dgst(3), nil),
		addContent(dgst(4), labelmap(string(labelGCContentRef), dgst(1).String())),
		addContent(dgst(5), labelmap(string(labelGCContentRef)+".anything-1", dgst(2).String(), string(labelGCContentRef)+".anything-2", dgst(3).String())),
		addContent(dgst(6), labelmap(string(labelGCContentRef)+"bad", dgst(1).String())),
		addContent(dgst(7), labelmap(string(labelGCContentRef)+"/anything-1", dgst(2).String(), string(labelGCContentRef)+"/anything-2", dgst(3).String())),
		addContent(dgst(11), nil),
		addContent(dgst(12), nil),
		addIngest("ingest-1", "", nil),
		addIngest("ingest-2", dgst(8), nil),
		/*
			addSnapshot("overlay", "sn1", "", nil),
			addSnapshot("overlay", "sn2", "sn1", nil),
			addSnapshot("overlay", "sn3", "sn2", nil),
			addSnapshot("overlay", "sn4", "", labelmap(string(labelGCSnapRef)+"btrfs", "sn1", string(labelGCSnapRef)+"overlay", "sn1")),
			addSnapshot("overlay", "sn5", "", labelmap(string(labelGCSnapRef)+"overlay/anything-1", "sn1", string(labelGCSnapRef)+"overlay/anything-2", "sn2")),
			addSnapshot("btrfs", "sn1", "", nil),
			addSnapshot("overlay", "sn1", "", nil),
			addSnapshot("overlay", "sn2", "sn1", nil),
			addSnapshot("overlay", "sn3", "", labelmap(
				string(labelGCContentRef), dgst(1).String(),
				string(labelGCContentRef)+".keep-me", dgst(6).String())),
		*/

		// Test flat references don't follow label references
		addContent(dgst(21), nil),
		addContent(dgst(22), labelmap(string(labelGCContentRef)+".0", dgst(1).String())),

		/*
			addSnapshot("overlay", "sn1", "", nil),
			addSnapshot("overlay", "sn2", "sn1", nil),
			addSnapshot("overlay", "sn3", "", labelmap(string(labelGCSnapRef)+"btrfs", "sn1", string(labelGCSnapRef)+"overlay", "sn1")),
		*/
	}

	refs := map[gc.Node][]gc.Node{
		gcnode(ResourceContent, dgst(1).String()): nil,
		gcnode(ResourceContent, dgst(2).String()): nil,
		gcnode(ResourceContent, dgst(3).String()): nil,
		gcnode(ResourceContent, dgst(4).String()): {
			gcnode(ResourceContent, dgst(1).String()),
		},
		gcnode(ResourceContent, dgst(5).String()): {
			gcnode(ResourceContent, dgst(2).String()),
			gcnode(ResourceContent, dgst(3).String()),
		},
		gcnode(ResourceContent, dgst(6).String()): nil,
		gcnode(ResourceContent, dgst(7).String()): {
			gcnode(ResourceContent, dgst(2).String()),
			gcnode(ResourceContent, dgst(3).String()),
		},
		gcnode(ResourceContent, dgst(11).String()): nil,
		gcnode(ResourceContent, dgst(12).String()): nil,
		/*
			gcnode(ResourceSnapshot, "overlay/sn1"):   nil,
			gcnode(ResourceSnapshot, "overlay/sn2"): {
				gcnode(ResourceSnapshot, "overlay/sn1"),
			},
			gcnode(ResourceSnapshot, "overlay/sn3"): {
				gcnode(ResourceSnapshot, "overlay/sn2"),
			},
			gcnode(ResourceSnapshot, "overlay/sn4"): {
				gcnode(ResourceSnapshot, "btrfs/sn1"),
				gcnode(ResourceSnapshot, "overlay/sn1"),
			},
			gcnode(ResourceSnapshot, "overlay/sn5"): {
				gcnode(ResourceSnapshot, "overlay/sn1"),
				gcnode(ResourceSnapshot, "overlay/sn2"),
			},
			gcnode(ResourceSnapshot, "btrfs/sn1"):   nil,
			gcnode(ResourceSnapshot, "overlay/sn1"): nil,
			gcnode(ResourceSnapshot, "overlay/sn2"): {
				gcnode(ResourceSnapshot, "overlay/sn1"),
			},
			gcnode(ResourceSnapshot, "overlay/sn3"): {
				gcnode(ResourceContent, dgst(1).String()),
				gcnode(ResourceContent, dgst(6).String()),
			},
		*/
		gcnode(ResourceIngest, "ingest-1"): nil,
		gcnode(ResourceIngest, "ingest-2"): {
			gcnode(ResourceContent, dgst(8).String()),
		},
		/*
			gcnode(resourceSnapshotFlat, "overlay/sn2"): {
				gcnode(resourceSnapshotFlat, "overlay/sn1"),
			},
			gcnode(ResourceSnapshot, "overlay/sn2"): {
				gcnode(ResourceSnapshot, "overlay/sn1"),
			},
			gcnode(resourceSnapshotFlat, "overlay/sn1"): nil,
			gcnode(resourceSnapshotFlat, "overlay/sn3"): nil,
			gcnode(ResourceSnapshot, "overlay/sn3"): {
				gcnode(ResourceSnapshot, "btrfs/sn1"),
				gcnode(ResourceSnapshot, "overlay/sn1"),
			},
		*/
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		v1bkt, err := tx.CreateBucketIfNotExists(bucketKeyVersion)
		if err != nil {
			return err
		}
		for _, alter := range alters {
			if err := alter(v1bkt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update failed: %+v", err)
	}

	ctx := context.Background()
	c := startGCContext(ctx, nil)

	for n, nodes := range refs {
		t.Log(n, nodes)
		checkNodeC(ctx, t, db, nodes, func(ctx context.Context, tx *bolt.Tx, nc chan<- gc.Node) error {
			return c.references(ctx, tx, n, func(n gc.Node) {
				select {
				case nc <- n:
				case <-ctx.Done():
				}
			})
		})
		if t.Failed() {
			t.Fatalf("Failure scanning %v", n)
		}
	}
}

func TestCollectibleResources(t *testing.T) {
	db, err := newDatabase(t)
	require.NoError(t, err)

	testResource := gc.ResourceType(0x10)

	alters := []alterFunc{
		addContent(dgst(1), nil),
		addImage("image1", dgst(1), nil),
		addContent(dgst(2), map[string]string{
			"containerd.io/gc.ref.test": "test2",
		}),
		addImage("image2", dgst(2), nil),
		addLease("lease1", labelmap(string(labelGCExpire), time.Now().Add(time.Hour).Format(time.RFC3339))),
		addLease("lease2", labelmap(string(labelGCExpire), time.Now().Add(-1*time.Hour).Format(time.RFC3339))),
	}
	refs := map[gc.Node][]gc.Node{
		gcnode(ResourceContent, dgst(1).String()): nil,
		gcnode(ResourceContent, dgst(2).String()): {
			gcnode(testResource, "test2"),
		},
	}
	all := []gc.Node{
		gcnode(ResourceContent, dgst(1).String()),
		gcnode(ResourceContent, dgst(2).String()),
		gcnode(ResourceLease, "lease1"),
		gcnode(ResourceLease, "lease2"),
		gcnode(testResource, "test1"),
		gcnode(testResource, "test2"), // 5: Will be removed
		gcnode(testResource, "test3"),
		gcnode(testResource, "test4"),
	}
	removeIndex := 5
	roots := []gc.Node{
		gcnode(ResourceContent, dgst(1).String()),
		gcnode(ResourceContent, dgst(2).String()),
		gcnode(ResourceLease, "lease1"),
		gcnode(testResource, "test1"),
		gcnode(testResource, "test3"),
	}
	collector := &testCollector{
		all: []gc.Node{
			gcnode(testResource, "test1"),
			gcnode(testResource, "test2"),
			gcnode(testResource, "test3"),
			gcnode(testResource, "test4"),
		},
		active: []gc.Node{
			gcnode(testResource, "test1"),
		},
		leased: map[string][]gc.Node{
			"lease1": {
				gcnode(testResource, "test3"),
			},
			"lease2": {
				gcnode(testResource, "test4"),
			},
		},
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		v1bkt, err := tx.CreateBucketIfNotExists(bucketKeyVersion)
		if err != nil {
			return err
		}
		for _, alter := range alters {
			if err := alter(v1bkt); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update failed: %+v", err)
	}

	ctx := context.Background()
	c := startGCContext(ctx, map[gc.ResourceType]Collector{
		testResource: collector,
	})

	for n, nodes := range refs {
		checkNodeC(ctx, t, db, nodes, func(ctx context.Context, tx *bolt.Tx, nc chan<- gc.Node) error {
			return c.references(ctx, tx, n, func(n gc.Node) {
				select {
				case nc <- n:
				case <-ctx.Done():
				}
			})
		})
		if t.Failed() {
			t.Fatalf("Failure scanning %v", n)
		}
	}
	checkNodes(ctx, t, db, all, func(ctx context.Context, tx *bolt.Tx, fn func(context.Context, gc.Node) error) error {
		return c.scanAll(ctx, tx, fn)
	})
	checkNodeC(ctx, t, db, roots, func(ctx context.Context, tx *bolt.Tx, nc chan<- gc.Node) error {
		return c.scanRoots(ctx, tx, nc)
	})

	if err := db.Update(func(tx *bolt.Tx) error {
		if err := c.remove(ctx, tx, all[removeIndex]); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("Update failed: %+v", err)
	}
	all = append(all[:removeIndex], all[removeIndex+1:]...)
	checkNodes(ctx, t, db, all, func(ctx context.Context, tx *bolt.Tx, fn func(context.Context, gc.Node) error) error {
		return c.scanAll(ctx, tx, fn)
	})
}

type testCollector struct {
	all    []gc.Node
	active []gc.Node
	leased map[string][]gc.Node
}

func (tc *testCollector) StartCollection(context.Context) (CollectionContext, error) {
	return tc, nil
}

func (tc *testCollector) ReferenceLabel() string {
	return "test"
}

func (tc *testCollector) All(fn func(gc.Node)) {
	for _, n := range tc.all {
		fn(n)
	}
}

func (tc *testCollector) Active(namespace string, fn func(gc.Node)) {
	for _, n := range tc.active {
		if n.Namespace == namespace {
			fn(n)
		}
	}
}

func (tc *testCollector) Leased(namespace, lease string, fn func(gc.Node)) {
	for _, n := range tc.leased[lease] {
		if n.Namespace == namespace {
			fn(n)
		}
	}
}

func (tc *testCollector) Remove(n gc.Node) {
	for i := range tc.all {
		if tc.all[i] == n {
			tc.all = append(tc.all[:i], tc.all[i+1:]...)
			return
		}
	}
}

func (tc *testCollector) Cancel() error {
	return nil
}

func (tc *testCollector) Finish() error {
	return nil
}

func newDatabase(t testing.TB) (*bolt.DB, error) {
	td := t.TempDir()

	db, err := bolt.Open(filepath.Join(td, "test.db"), 0777, nil)
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	return db, nil
}

func checkNodeC(ctx context.Context, t *testing.T, db *bolt.DB, expected []gc.Node, fn func(context.Context, *bolt.Tx, chan<- gc.Node) error) {
	t.Helper()
	var actual []gc.Node
	nc := make(chan gc.Node)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := range nc {
			actual = append(actual, n)
		}
	}()
	if err := db.View(func(tx *bolt.Tx) error {
		defer close(nc)
		return fn(ctx, tx, nc)
	}); err != nil {
		t.Fatal(err)
	}

	<-done
	checkNodesEqual(t, actual, expected)
}

func checkNodes(ctx context.Context, t *testing.T, db *bolt.DB, expected []gc.Node, fn func(context.Context, *bolt.Tx, func(context.Context, gc.Node) error) error) {
	t.Helper()
	var actual []gc.Node
	scanFn := func(ctx context.Context, n gc.Node) error {
		actual = append(actual, n)
		return nil
	}

	if err := db.View(func(tx *bolt.Tx) error {
		return fn(ctx, tx, scanFn)
	}); err != nil {
		t.Fatal(err)
	}

	checkNodesEqual(t, actual, expected)
}

func checkNodesEqual(t *testing.T, n1, n2 []gc.Node) {
	t.Helper()
	sort.Sort(nodeList(n1))
	sort.Sort(nodeList(n2))

	if len(n1) != len(n2) {
		t.Fatalf("Nodes do not match\n\tExpected:\n\t%v\n\tActual:\n\t%v", n2, n1)
	}

	for i := range n1 {
		if n1[i] != n2[i] {
			t.Errorf("[%d] root does not match expected: expected %v, got %v", i, n2[i], n1[i])
		}
	}
}

type nodeList []gc.Node

func (nodes nodeList) Len() int {
	return len(nodes)
}

func (nodes nodeList) Less(i, j int) bool {
	if nodes[i].Type != nodes[j].Type {
		return nodes[i].Type < nodes[j].Type
	}
	if nodes[i].Namespace != nodes[j].Namespace {
		return nodes[i].Namespace < nodes[j].Namespace
	}
	return nodes[i].Key < nodes[j].Key
}

func (nodes nodeList) Swap(i, j int) {
	nodes[i], nodes[j] = nodes[j], nodes[i]
}

type alterFunc func(bkt *bolt.Bucket) error

func addImage(name string, dgst digest.Digest, labels map[string]string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		ibkt, err := createBuckets(bkt, string(bucketKeyObjectImages), name)
		if err != nil {
			return err
		}

		tbkt, err := ibkt.CreateBucket(bucketKeyTarget)
		if err != nil {
			return err
		}
		if err := tbkt.Put(bucketKeyDigest, []byte(dgst.String())); err != nil {
			return err
		}

		return boltutil.WriteLabels(ibkt, labels)
	}
}

/*
func addSnapshot(snapshotter, name, parent string, labels map[string]string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		sbkt, err := createBuckets(bkt, string(bucketKeyObjectSnapshots), snapshotter, name)
		if err != nil {
			return err
		}
		if parent != "" {
			if err := sbkt.Put(bucketKeyParent, []byte(parent)); err != nil {
				return err
			}
		}
		return boltutil.WriteLabels(sbkt, labels)
	}
}
*/

func addContent(dgst digest.Digest, labels map[string]string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		cbkt, err := createBuckets(bkt, string(bucketKeyObjectContent), string(bucketKeyObjectBlob), dgst.String())
		if err != nil {
			return err
		}
		return boltutil.WriteLabels(cbkt, labels)
	}
}

func addIngest(ref string, expected digest.Digest, expires *time.Time) alterFunc {
	return func(bkt *bolt.Bucket) error {
		cbkt, err := createBuckets(bkt, string(bucketKeyObjectContent), string(bucketKeyObjectIngests), ref)
		if err != nil {
			return err
		}
		if expected != "" {
			if err := cbkt.Put(bucketKeyExpected, []byte(expected)); err != nil {
				return err
			}
		}
		if expires != nil {
			if err := writeExpireAt(*expires, cbkt); err != nil {
				return err
			}
		}
		return nil
	}
}

func addLease(lid string, labels map[string]string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		lbkt, err := createBuckets(bkt, string(bucketKeyObjectLeases), lid)
		if err != nil {
			return err
		}
		return boltutil.WriteLabels(lbkt, labels)
	}
}

/*
func addLeaseSnapshot(lid, snapshotter, name string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		sbkt, err := createBuckets(bkt, string(bucketKeyObjectLeases), lid, string(bucketKeyObjectSnapshots), snapshotter)
		if err != nil {
			return err
		}
		return sbkt.Put([]byte(name), nil)
	}
}
*/

func addLeaseContent(lid string, dgst digest.Digest) alterFunc {
	return func(bkt *bolt.Bucket) error {
		cbkt, err := createBuckets(bkt, string(bucketKeyObjectLeases), lid, string(bucketKeyObjectContent))
		if err != nil {
			return err
		}
		return cbkt.Put([]byte(dgst.String()), nil)
	}
}

func addLeaseIngest(lid, ref string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		cbkt, err := createBuckets(bkt, string(bucketKeyObjectLeases), lid, string(bucketKeyObjectIngests))
		if err != nil {
			return err
		}
		return cbkt.Put([]byte(ref), nil)
	}
}

/*
func addContainer(name, snapshotter, snapshot string, labels map[string]string) alterFunc {
	return func(bkt *bolt.Bucket) error {
		cbkt, err := createBuckets(bkt, string(bucketKeyObjectContainers), name)
		if err != nil {
			return err
		}
		if err := cbkt.Put(bucketKeySnapshotter, []byte(snapshotter)); err != nil {
			return err
		}
		if err := cbkt.Put(bucketKeySnapshotKey, []byte(snapshot)); err != nil {
			return err
		}
		return boltutil.WriteLabels(cbkt, labels)
	}
}
*/

func createBuckets(bkt *bolt.Bucket, names ...string) (*bolt.Bucket, error) {
	for _, name := range names {
		nbkt, err := bkt.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return nil, err
		}
		bkt = nbkt
	}
	return bkt, nil
}

func labelmap(kv ...string) map[string]string {
	if len(kv)%2 != 0 {
		panic("bad labels argument")
	}
	l := map[string]string{}
	for i := 0; i < len(kv); i = i + 2 {
		l[kv[i]] = kv[i+1]
	}
	return l
}

func dgst(i int64) digest.Digest {
	r := rand.New(rand.NewSource(i))
	dgstr := digest.SHA256.Digester()
	if _, err := io.CopyN(dgstr.Hash(), r, 256); err != nil {
		panic(err)
	}
	return dgstr.Digest()
}

func timeIn(d time.Duration) *time.Time {
	t := time.Now().UTC().Add(d)
	return &t
}
