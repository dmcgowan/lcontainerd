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
	"path/filepath"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	localcontent "github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/gc"
	"github.com/containerd/containerd/log"
	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

const (
	// schemaVersion represents the schema version of
	// the database. This schema version represents the
	// structure of the data in the database. The schema
	// can envolve at any time but any backwards
	// incompatible changes or structural changes require
	// bumping the schema version.
	schemaVersion = "v1"

	// dbVersion represents updates to the schema
	// version which are additions and compatible with
	// prior version of the same schema.
	dbVersion = 1
)

// DBOpt configures how we set up the DB
type DBOpt func(*dbOptions)

// dbOptions configure db options.
type dbOptions struct {
	boltOptions bbolt.Options
}

func WithReadOnly(dbo *dbOptions) {
	dbo.boltOptions.ReadOnly = true
}

// DB represents a metadata database backed by a bolt
// database. The database is fully namespaced and stores
// image, container, namespace, snapshot, and content data
// while proxying data shared across namespaces to backend
// datastores for content and snapshots.
type DB struct {
	db *bolt.DB
	cs *contentStore

	// wlock is used to protect access to the data structures during garbage
	// collection. While the wlock is held no writable transactions can be
	// opened, preventing changes from occurring between the mark and
	// sweep phases without preventing read transactions.
	wlock sync.RWMutex

	// dirty flag indicates that references have been removed which require
	// a garbage collection to ensure the database is clean. This tracks
	// the number of dirty operations. This should be updated and read
	// atomically if outside of wlock.Lock.
	dirty uint32

	// dirtyCS flags keeps track of datastores which have had
	// deletions since the last garbage collection. These datastores will
	// be garbage collected during the next garbage collection. These
	// should only be updated inside of a write transaction or wlock.Lock.
	dirtyCS bool

	// collectible resources
	collectors map[gc.ResourceType]Collector

	dbopts dbOptions
}

// NewDB creates a new metadata database using the provided
// bolt database, content store, and snapshotters.
func NewDB(root string, opts ...DBOpt) (*DB, error) {
	var dbo dbOptions
	for _, opt := range opts {
		opt(&dbo)
	}

	metadb := filepath.Join(root, "meta.db")
	bdb, err := bbolt.Open(metadb, 0600, &dbo.boltOptions)
	if err != nil {
		return nil, err
	}

	contentpath := filepath.Join(root, "content")
	cs, err := localcontent.NewStore(contentpath)
	if err != nil {
		return nil, err
	}

	m := &DB{
		db:     bdb,
		dbopts: dbo,
	}

	m.cs = newContentStore(m, cs)

	return m, nil
}

func (m *DB) Close(ctx context.Context) error {
	_, gcerr := m.GarbageCollect(ctx)
	cerr := m.db.Close()
	if gcerr != nil {
		return gcerr
	}
	return cerr
}

// ContentStore returns a namespaced content store
// proxied to a content store.
func (m *DB) ContentStore() content.Store {
	if m.cs == nil {
		return nil
	}
	return m.cs
}

// View runs a readonly transaction on the metadata store.
func (m *DB) View(fn func(*bolt.Tx) error) error {
	return m.db.View(fn)
}

// Update runs a writable transaction on the metadata store.
func (m *DB) Update(fn func(*bolt.Tx) error) error {
	m.wlock.RLock()
	defer m.wlock.RUnlock()
	err := m.db.Update(func(tx *bolt.Tx) error {
		if err := updateDBVersion(tx); err != nil {
			return err
		}

		return fn(tx)
		// TODO: Check for cleanup?
	})
	return err
}
func updateDBVersion(tx *bolt.Tx) error {
	var (
		bkt = tx.Bucket([]byte(schemaVersion))
		err error
		vb  []byte
	)
	if bkt != nil {
		vb = bkt.Get(bucketKeyDBVersion)
	} else {
		bkt, err = tx.CreateBucket([]byte(schemaVersion))
		if err != nil {
			return err
		}
	}
	if vb == nil {
		versionEncoded, err := encodeInt(dbVersion)
		if err != nil {
			return err
		}

		return bkt.Put(bucketKeyDBVersion, versionEncoded)
	} else {
		v, _ := binary.Varint(vb)
		if v != dbVersion {
			// Here is where migration can happen when bumping the version,
			// currently only one version exists so not necesesary
			return fmt.Errorf("wrong version: %d: %w", v, errdefs.ErrFailedPrecondition)
		}

		return nil
	}
}

// GCStats holds the duration for the different phases of the garbage collector
type GCStats struct {
	MetaD     time.Duration
	ContentD  time.Duration
	SnapshotD map[string]time.Duration
}

// Elapsed returns the duration which elapsed during a collection
func (s GCStats) Elapsed() time.Duration {
	return s.MetaD
}

// GarbageCollect removes resources (snapshots, contents, ...) that are no longer used.
func (m *DB) GarbageCollect(ctx context.Context) (gc.Stats, error) {
	m.wlock.Lock()
	t1 := time.Now()
	c := startGCContext(ctx, m.collectors)

	marked, err := m.getMarked(ctx, c) // Pass in gc context
	if err != nil {
		m.wlock.Unlock()
		return nil, err
	}

	if err := m.db.Update(func(tx *bolt.Tx) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		rm := func(ctx context.Context, n gc.Node) error {
			if _, ok := marked[n]; ok {
				return nil
			}

			if n.Type == ResourceSnapshot {
				//if idx := strings.IndexRune(n.Key, '/'); idx > 0 {
				//	m.dirtySS[n.Key[:idx]] = struct{}{}
				//}
			} else if n.Type == ResourceContent || n.Type == ResourceIngest {
				m.dirtyCS = true
			}
			return c.remove(ctx, tx, n) // From gc context
		}

		if err := c.scanAll(ctx, tx, rm); err != nil { // From gc context
			return fmt.Errorf("failed to scan and remove: %w", err)
		}

		return nil
	}); err != nil {
		m.wlock.Unlock()
		c.cancel(ctx)
		return nil, err
	}

	var stats GCStats
	var wg sync.WaitGroup

	// reset dirty, no need for atomic inside of wlock.Lock
	m.dirty = 0

	/*
		if len(m.dirtySS) > 0 {
			var sl sync.Mutex
			stats.SnapshotD = map[string]time.Duration{}
			wg.Add(len(m.dirtySS))
			for snapshotterName := range m.dirtySS {
				log.G(ctx).WithField("snapshotter", snapshotterName).Debug("schedule snapshotter cleanup")
				go func(snapshotterName string) {
					st1 := time.Now()
					m.cleanupSnapshotter(snapshotterName)

					sl.Lock()
					stats.SnapshotD[snapshotterName] = time.Since(st1)
					sl.Unlock()

					wg.Done()
				}(snapshotterName)
			}
			m.dirtySS = map[string]struct{}{}
		}
	*/

	if m.dirtyCS {
		wg.Add(1)
		log.G(ctx).Debug("schedule content cleanup")
		go func() {
			ct1 := time.Now()
			m.cleanupContent()
			stats.ContentD = time.Since(ct1)
			wg.Done()
		}()
		m.dirtyCS = false
	}

	stats.MetaD = time.Since(t1)
	m.wlock.Unlock()

	c.finish(ctx)

	wg.Wait()

	return stats, err
}

// getMarked returns all resources that are used.
func (m *DB) getMarked(ctx context.Context, c *gcContext) (map[gc.Node]struct{}, error) {
	var marked map[gc.Node]struct{}
	if err := m.db.View(func(tx *bolt.Tx) error {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		var (
			nodes []gc.Node
			wg    sync.WaitGroup
			roots = make(chan gc.Node)
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range roots {
				nodes = append(nodes, n)
			}
		}()
		// Call roots
		if err := c.scanRoots(ctx, tx, roots); err != nil { // From gc context
			cancel()
			return err
		}
		close(roots)
		wg.Wait()

		refs := func(n gc.Node) ([]gc.Node, error) {
			var sn []gc.Node
			if err := c.references(ctx, tx, n, func(nn gc.Node) { // From gc context
				sn = append(sn, nn)
			}); err != nil {
				return nil, err
			}
			return sn, nil
		}

		reachable, err := gc.Tricolor(nodes, refs)
		if err != nil {
			return err
		}
		marked = reachable
		return nil
	}); err != nil {
		return nil, err
	}
	return marked, nil
}

/*
func (m *DB) cleanupSnapshotter(name string) (time.Duration, error) {
	ctx := context.Background()
	sn, ok := m.ss[name]
	if !ok {
		return 0, nil
	}

	d, err := sn.garbageCollect(ctx)
	logger := log.G(ctx).WithField("snapshotter", name)
	if err != nil {
		logger.WithError(err).Warn("snapshot garbage collection failed")
	} else {
		logger.WithField("d", d).Debugf("snapshot garbage collected")
	}
	return d, err
}
*/

func (m *DB) cleanupContent() (time.Duration, error) {
	ctx := context.Background()
	if m.cs == nil {
		return 0, nil
	}

	d, err := m.cs.garbageCollect(ctx)
	if err != nil {
		log.G(ctx).WithError(err).Warn("content garbage collection failed")
	} else {
		log.G(ctx).WithField("d", d).Debugf("content garbage collected")
	}

	return d, err
}
