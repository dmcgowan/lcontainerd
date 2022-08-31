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
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/leases"
	"github.com/containerd/containerd/log/logtest"
	"github.com/containerd/containerd/namespaces"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
)

type testOptions struct {
}

type testOpt func(*testOptions)

func testDB(t *testing.T, opt ...testOpt) (context.Context, *DB) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = namespaces.WithNamespace(ctx, "testing")
	ctx = logtest.WithT(ctx, t)

	var topts testOptions

	for _, o := range opt {
		o(&topts)
	}

	dirname := t.TempDir()

	db, err := NewDB(dirname)
	if err != nil {
		t.Fatal(err)
	}
	//if err := db.Init(ctx); err != nil {
	//	t.Fatal(err)
	//}

	t.Cleanup(func() {
		db.Close()
		cancel()
	})
	return ctx, db
}

func TestInit(t *testing.T) {
	_, db := testEnv(t)

	if err := db.Update(func(*bolt.Tx) error { return nil }); err != nil {
		t.Fatal(err)
	}

	version, err := readDBVersion(db, bucketKeyVersion)
	if err != nil {
		t.Fatal(err)
	}
	if version != dbVersion {
		t.Fatalf("Unexpected version %d, expected %d", version, dbVersion)
	}
}

/*
func TestMigrations(t *testing.T) {
	testRefs := []struct {
		ref  string
		bref string
	}{
		{
			ref:  "k1",
			bref: "bk1",
		},
		{
			ref:  strings.Repeat("longerkey", 30), // 270 characters
			bref: "short",
		},
		{
			ref:  "short",
			bref: strings.Repeat("longerkey", 30), // 270 characters
		},
		{
			ref:  "emptykey",
			bref: "",
		},
	}
	migrationTests := []struct {
		name  string
		init  func(*bolt.Tx) error
		check func(*bolt.Tx) error
	}{
		{
			name: "IngestUpdate",
			init: func(tx *bolt.Tx) error {
				bkt, err := createBucketIfNotExists(tx, bucketKeyVersion, []byte("testing"), bucketKeyObjectContent, deprecatedBucketKeyObjectIngest)
				if err != nil {
					return err
				}

				for _, s := range testRefs {
					if err := bkt.Put([]byte(s.ref), []byte(s.bref)); err != nil {
						return err
					}
				}

				return nil
			},
			check: func(tx *bolt.Tx) error {
				bkt := getIngestsBucket(tx, "testing")
				if bkt == nil {
					return fmt.Errorf("ingests bucket not found: %w", errdefs.ErrNotFound)
				}

				for _, s := range testRefs {
					sbkt := bkt.Bucket([]byte(s.ref))
					if sbkt == nil {
						return fmt.Errorf("ref does not exist: %w", errdefs.ErrNotFound)
					}

					bref := string(sbkt.Get(bucketKeyRef))
					if bref != s.bref {
						return fmt.Errorf("unexpected reference key %q, expected %q", bref, s.bref)
					}
				}

				dbkt := getBucket(tx, bucketKeyVersion, []byte("testing"), bucketKeyObjectContent, deprecatedBucketKeyObjectIngest)
				if dbkt != nil {
					return errors.New("deprecated ingest bucket still exists")
				}

				return nil
			},
		},

		{
			name: "NoOp",
			init: func(tx *bolt.Tx) error {
				return nil
			},
			check: func(tx *bolt.Tx) error {
				return nil
			},
		},
	}

	if len(migrationTests) != len(migrations) {
		t.Fatal("Each migration must have a test case")
	}

	for i, mt := range migrationTests {
		t.Run(mt.name, runMigrationTest(i, mt.init, mt.check))
	}
}

func runMigrationTest(i int, init, check func(*bolt.Tx) error) func(t *testing.T) {
	return func(t *testing.T) {
		_, db, cancel := testEnv(t)
		defer cancel()

		if err := db.Update(init); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(migrations[i].migrate); err != nil {
			t.Fatal(err)
		}

		if err := db.View(check); err != nil {
			t.Fatal(err)
		}
	}
}
*/

func readDBVersion(db *DB, schema []byte) (int, error) {
	var version int
	if err := db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(schema)
		if bkt == nil {
			return fmt.Errorf("no version bucket: %w", errdefs.ErrNotFound)
		}
		vb := bkt.Get(bucketKeyDBVersion)
		if vb == nil {
			return fmt.Errorf("no version value: %w", errdefs.ErrNotFound)
		}
		v, _ := binary.Varint(vb)
		version = int(v)
		return nil
	}); err != nil {
		return 0, err
	}
	return version, nil
}

func bytesFor(i int64) []byte {
	r := rand.New(rand.NewSource(i))
	var b [256]byte
	_, err := r.Read(b[:])
	if err != nil {
		panic(err)
	}
	return b[:]
}

func digestFor(i int64) digest.Digest {
	r := rand.New(rand.NewSource(i))
	dgstr := digest.SHA256.Digester()
	_, err := io.Copy(dgstr.Hash(), io.LimitReader(r, 256))
	if err != nil {
		panic(err)
	}
	return dgstr.Digest()
}

type object struct {
	data    interface{}
	removed bool
	labels  map[string]string
}

/*
func create(obj object, tx *bolt.Tx, db *DB, cs content.Store, sn snapshots.Snapshotter) (*gc.Node, error) {
	var (
		node      *gc.Node
		namespace = "test"
		ctx       = WithTransactionContext(namespaces.WithNamespace(context.Background(), namespace), tx)
	)

	switch v := obj.data.(type) {
	case testContent:
		expected := digest.FromBytes(v.data)
		w, err := cs.Writer(ctx,
			content.WithRef("test-ref"),
			content.WithDescriptor(ocispec.Descriptor{Size: int64(len(v.data)), Digest: expected}))
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}
		if _, err := w.Write(v.data); err != nil {
			return nil, fmt.Errorf("write blob failed: %w", err)
		}
		if err := w.Commit(ctx, int64(len(v.data)), expected, content.WithLabels(obj.labels)); err != nil {
			return nil, fmt.Errorf("failed to commit blob: %w", err)
		}
		if !obj.removed {
			node = &gc.Node{
				Type:      ResourceContent,
				Namespace: namespace,
				Key:       expected.String(),
			}
		}
	case testSnapshot:
		if v.active {
			_, err := sn.Prepare(ctx, v.key, v.parent, snapshots.WithLabels(obj.labels))
			if err != nil {
				return nil, err
			}
		} else {
			akey := fmt.Sprintf("%s-active", v.key)
			_, err := sn.Prepare(ctx, akey, v.parent)
			if err != nil {
				return nil, err
			}
			if err := sn.Commit(ctx, v.key, akey, snapshots.WithLabels(obj.labels)); err != nil {
				return nil, err
			}
		}
		if !obj.removed {
			node = &gc.Node{
				Type:      ResourceSnapshot,
				Namespace: namespace,
				Key:       fmt.Sprintf("native/%s", v.key),
			}
		}
	case testImage:
		image := images.Image{
			Name:   v.name,
			Target: v.target,
			Labels: obj.labels,
		}

		_, err := NewImageStore(db).Create(ctx, image)
		if err != nil {
			return nil, fmt.Errorf("failed to create image: %w", err)
		}
	case testContainer:
		container := containers.Container{
			ID:          v.id,
			SnapshotKey: v.snapshot,
			Snapshotter: "native",
			Labels:      obj.labels,

			Runtime: containers.RuntimeInfo{
				Name: "testruntime",
			},
			Spec: &types.Any{},
		}
		_, err := NewContainerStore(db).Create(ctx, container)
		if err != nil {
			return nil, err
		}
	case testLease:
		lm := NewLeaseManager(db)

		l, err := lm.Create(ctx, leases.WithID(v.id), leases.WithLabels(obj.labels))
		if err != nil {
			return nil, err
		}

		for _, ref := range v.refs {
			if err := lm.AddResource(ctx, l, ref); err != nil {
				return nil, err
			}
		}

		if !obj.removed {
			node = &gc.Node{
				Type:      ResourceLease,
				Namespace: namespace,
				Key:       v.id,
			}
		}
	}

	return node, nil
}

func blob(b []byte, r bool, l ...string) object {
	return object{
		data: testContent{
			data: b,
		},
		removed: r,
		labels:  labelmap(l...),
	}
}

func image(n string, d digest.Digest, l ...string) object {
	return object{
		data: testImage{
			name: n,
			target: ocispec.Descriptor{
				MediaType: "irrelevant",
				Digest:    d,
				Size:      256,
			},
		},
		removed: false,
		labels:  labelmap(l...),
	}
}

func newSnapshot(key, parent string, active, r bool, l ...string) object {
	return object{
		data: testSnapshot{
			key:    key,
			parent: parent,
			active: active,
		},
		removed: r,
		labels:  labelmap(l...),
	}
}

func container(id, s string, l ...string) object {
	return object{
		data: testContainer{
			id:       id,
			snapshot: s,
		},
		removed: false,
		labels:  labelmap(l...),
	}
}

func lease(id string, refs []leases.Resource, r bool, l ...string) object {
	return object{
		data: testLease{
			id:   id,
			refs: refs,
		},
		removed: r,
		labels:  labelmap(l...),
	}
}
*/

type testContent struct {
	data []byte
}

type testSnapshot struct {
	key    string
	parent string
	active bool
}

type testImage struct {
	name   string
	target ocispec.Descriptor
}

type testContainer struct {
	id       string
	snapshot string
}

type testLease struct {
	id   string
	refs []leases.Resource
}

func newStores(t testing.TB) (*DB, content.Store) {
	mdb, err := NewDB(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		mdb.Close()
	})

	return mdb, mdb.ContentStore()
}

func testEnv(t *testing.T) (context.Context, *DB) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = logtest.WithT(ctx, t)

	dirname := t.TempDir()

	db, err := NewDB(dirname)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.Close()
		cancel()
	})

	return ctx, db
}
