package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type s3PartIndex struct {
	s3Layer  *s3Layer
	database string
	table    string
	layer    string
	layers   []s3Layer
	partPath string

	etag string

	entries   *sync.Map
	promises  []Promise[int32]
	m         sync.Mutex
	updateCtx context.Context
	doUpdate  context.CancelFunc
	workCtx   context.Context
	stop      context.CancelFunc
	lastId    uint32

	pendingAdd []*IndexEntry
	pendingRm  []*IndexEntry

	dropQueue        []DropPlan
	parquetSizeBytes int64
	rowCount         int64
	minTime          int64
	maxTime          int64
	filesInMerge     map[string]bool
	filesInMove      map[string]bool
}

var _ TableIndex = &s3PartIndex{}

type s3PartIdxOpts struct {
	s3Layer  *s3Layer
	database string
	table    string
	partPath string
	layers   []s3Layer
	layer    string
}

func newS3PartIndex(opts s3PartIdxOpts) (*s3PartIndex, error) {
	res := &s3PartIndex{
		s3Layer:      opts.s3Layer,
		database:     opts.database,
		table:        opts.table,
		partPath:     opts.partPath,
		layer:        opts.layer,
		layers:       opts.layers,
		entries:      &sync.Map{},
		filesInMerge: make(map[string]bool),
		filesInMove:  make(map[string]bool),
	}
	res.updateCtx, res.doUpdate = context.WithCancel(context.Background())
	res.workCtx, res.stop = context.WithCancel(context.Background())
	err := res.populate()
	return res, err
}

func (J *s3PartIndex) GetAll() ([]*IndexEntry, error) {
	var res []*IndexEntry
	J.entries.Range(func(key, value interface{}) bool {
		res = append(res, J.jEntry2Entry(value.(*jsonIndexEntry)))
		return true
	})
	return res, nil
}

func (J *s3PartIndex) getLayer(name string) int {
	for i, layer := range J.layers {
		if layer.Name == name {
			return i
		}
	}
	return -1
}

func (J *s3PartIndex) GetQuerier() TableQuerier {
	return J
}

func (J *s3PartIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	var res []*IndexEntry
	var suffix string
	if options.Iteration != 0 {
		suffix = fmt.Sprintf(".%d.parquet", options.Iteration)
	}
	J.entries.Range(func(key, value interface{}) bool {
		_v := value.(*jsonIndexEntry)
		if suffix != "" && !strings.HasSuffix(_v.Path, suffix) {
			return true
		}
		if options.Before.Unix() > 0 && _v.MinTime > options.Before.UnixNano() {
			return true
		}
		if options.After.Unix() > 0 && _v.MaxTime < options.After.UnixNano() {
			return true
		}
		res = append(res, J.jEntry2Entry(_v))
		return true
	})
	return res, nil
}

func (J *s3PartIndex) addToDropQueue(files []*IndexEntry) {
	for _, f := range files {
		J.dropQueue = append(J.dropQueue, DropPlan{
			WriterID: f.WriterID,
			Layer:    f.Layer,
			Database: f.Database,
			Table:    f.Table,
			Path:     f.Path,
			TimeS:    int32(time.Now().Add(time.Second * 30).Unix()),
		})
	}
}

func (J *s3PartIndex) populate() error {
	key := J.s3Layer.objectPath(J.database, J.table, J.partPath)
	data, etag, err := J.s3Layer.getObject(context.Background(), key)
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	J.etag = etag
	return J.parseMetadata(data)
}

func (J *s3PartIndex) parseMetadata(data []byte) error {
	iter := jsoniter.Parse(jsoniter.ConfigDefault, bytes.NewReader(data), 4096)
	var parseErr error
	iter.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
		switch s {
		case "drop_queue":
			for iterator.ReadArray() {
				var dropQueueEntry DropPlan
				iterator.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
					switch s {
					case "writer_id":
						dropQueueEntry.WriterID = iterator.ReadString()
					case "layer":
						dropQueueEntry.Layer = iterator.ReadString()
					case "database":
						dropQueueEntry.Database = iterator.ReadString()
					case "table":
						dropQueueEntry.Table = iterator.ReadString()
					case "path":
						dropQueueEntry.Path = iterator.ReadString()
					case "time_s":
						dropQueueEntry.TimeS = iterator.ReadInt32()
					default:
						iterator.Skip()
					}
					return true
				})
				J.dropQueue = append(J.dropQueue, dropQueueEntry)
			}
		case "type":
			iterator.Skip()
		case "parquet_size_bytes":
			J.parquetSizeBytes = iterator.ReadInt64()
		case "row_count":
			J.rowCount = iterator.ReadInt64()
		case "min_time":
			J.minTime = iterator.ReadInt64()
		case "max_time":
			J.maxTime = iterator.ReadInt64()
		case "wal_sequence":
			iterator.Skip()
		case "files":
			parseErr = J.populateFiles(iterator)
			if parseErr != nil {
				return false
			}
		}
		return true
	})
	if parseErr != nil {
		return parseErr
	}
	if iter.Error != nil {
		return iter.Error
	}
	return nil
}

func (J *s3PartIndex) populateFiles(iter *jsoniter.Iterator) error {
	for iter.ReadArray() {
		e := &jsonIndexEntry{}
		iter.ReadVal(e)
		_marshalled, err := json.Marshal(e)
		if err != nil {
			return err
		}
		e._marshalled = string(_marshalled)
		if e.Id > J.lastId {
			J.lastId = e.Id
		}
		J.entries.Store(e.Path, e)
	}
	return nil
}

func (J *s3PartIndex) Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	_add, err := J.entry2JEntry(add)
	if err != nil {
		return Fulfilled[int32](err, 0)
	}
	J.m.Lock()
	defer J.m.Unlock()
	J.add(_add)
	removed := J.rm(rm)
	if len(_add) == 0 && !removed {
		return Fulfilled(nil, int32(0))
	}
	// Track pending changes for conflict resolution
	J.pendingAdd = append(J.pendingAdd, add...)
	J.pendingRm = append(J.pendingRm, rm...)
	J.addToDropQueue(rm)
	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *s3PartIndex) entry2JEntry(entries []*IndexEntry) ([]*jsonIndexEntry, error) {
	res := make([]*jsonIndexEntry, len(entries))
	for i, entry := range entries {
		id := atomic.AddUint32(&J.lastId, 1)
		_entry := &jsonIndexEntry{
			Id:         id,
			IndexEntry: *entry,
			Range:      "1h",
			Type:       "compacted",
		}
		_marshalled, err := json.Marshal(_entry)
		if err != nil {
			return nil, err
		}
		_entry._marshalled = string(_marshalled)
		res[i] = _entry
	}
	return res, nil
}

func (J *s3PartIndex) add(entries []*jsonIndexEntry) {
	for _, entry := range entries {
		J.rowCount += entry.RowCount
		J.parquetSizeBytes += entry.SizeBytes
		J.entries.Store(entry.Path, entry)
		if entry.Id == 1 {
			J.minTime = entry.MinTime
			J.maxTime = entry.MaxTime
			continue
		}
		if entry.MinTime != 0 {
			J.minTime = min(J.minTime, entry.MinTime)
		}
		if entry.MinTime != 0 {
			J.maxTime = max(J.maxTime, entry.MaxTime)
		}
	}
}

func (J *s3PartIndex) recalcMin() {
	if J.entries == nil {
		J.minTime = 0
		return
	}
	var i int
	J.entries.Range(func(key, value interface{}) bool {
		entry := value.(*jsonIndexEntry)
		if i == 0 {
			J.minTime = entry.MinTime
			i++
		}
		J.minTime = min(J.minTime, entry.MinTime)
		return true
	})
}

func (J *s3PartIndex) recalcMax() {
	if J.entries == nil {
		J.maxTime = 0
		return
	}
	var i int
	J.entries.Range(func(key, value interface{}) bool {
		entry := value.(*jsonIndexEntry)
		if i == 0 {
			J.maxTime = entry.MaxTime
			i++
		}
		J.maxTime = max(J.maxTime, entry.MaxTime)
		return true
	})
}

func (J *s3PartIndex) rm(entries []*IndexEntry) bool {
	rm := false
	for _, entry := range entries {
		e, ok := J.entries.Load(entry.Path)
		if !ok {
			continue
		}
		_e := e.(*jsonIndexEntry)
		rm = true
		J.rowCount -= _e.RowCount
		J.parquetSizeBytes -= _e.SizeBytes
		J.entries.Delete(entry.Path)
		if _e.MinTime == J.minTime {
			J.recalcMin()
		}
		if _e.MaxTime == J.maxTime {
			J.recalcMax()
		}
	}
	return rm
}

func (J *s3PartIndex) serializeToBytes() ([]byte, error) {
	var buf bytes.Buffer
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)

	stream.WriteObjectStart()

	stream.WriteObjectField("type")
	stream.WriteString(J.table)

	stream.WriteMore()
	stream.WriteObjectField("parquet_size_bytes")
	stream.WriteInt64(J.parquetSizeBytes)

	stream.WriteMore()
	stream.WriteObjectField("row_count")
	stream.WriteInt64(J.rowCount)

	stream.WriteMore()
	stream.WriteObjectField("min_time")
	stream.WriteInt64(J.minTime)

	stream.WriteMore()
	stream.WriteObjectField("max_time")
	stream.WriteInt64(J.maxTime)

	stream.WriteMore()
	stream.WriteObjectField("wal_sequence")
	stream.WriteInt64(0)

	stream.WriteMore()
	stream.WriteObjectField("drop_queue")
	stream.WriteArrayStart()
	for i, d := range J.dropQueue {
		if i > 0 {
			stream.WriteMore()
		}
		strD, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		stream.WriteRaw(string(strD))
	}
	stream.WriteArrayEnd()

	stream.WriteMore()
	stream.WriteObjectField("files")
	stream.WriteArrayStart()
	var entryIdx int
	J.entries.Range(func(key, value any) bool {
		if entryIdx > 0 {
			stream.WriteMore()
		}
		stream.WriteRaw(value.(*jsonIndexEntry)._marshalled)
		entryIdx++
		return true
	})
	stream.WriteArrayEnd()
	stream.WriteObjectEnd()

	if stream.Error != nil {
		return nil, stream.Error
	}
	err := stream.Flush()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

const maxFlushRetries = 3

func (J *s3PartIndex) flush() {
	J.m.Lock()
	J.updateCtx, J.doUpdate = context.WithCancel(context.Background())
	promises := J.promises
	J.promises = nil
	pendingAdd := J.pendingAdd
	pendingRm := J.pendingRm
	J.pendingAdd = nil
	J.pendingRm = nil
	J.m.Unlock()

	onErr := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}

	for attempt := 0; attempt < maxFlushRetries; attempt++ {
		data, err := J.serializeToBytes()
		if err != nil {
			onErr(err)
			return
		}

		key := J.s3Layer.objectPath(J.database, J.table, J.partPath)
		newETag, err := J.s3Layer.checkAndPut(context.Background(), key, data, J.etag)
		if err == nil {
			J.m.Lock()
			J.etag = newETag
			J.m.Unlock()
			onErr(nil)
			return
		}

		if err != errETagMismatch {
			onErr(err)
			return
		}

		// ETag mismatch: re-read, merge, retry
		err = J.reloadAndMerge(pendingAdd, pendingRm)
		if err != nil {
			onErr(err)
			return
		}
	}

	onErr(fmt.Errorf("s3: flush failed after %d retries due to concurrent modifications", maxFlushRetries))
}

func (J *s3PartIndex) reloadAndMerge(pendingAdd []*IndexEntry, pendingRm []*IndexEntry) error {
	key := J.s3Layer.objectPath(J.database, J.table, J.partPath)
	data, etag, err := J.s3Layer.getObject(context.Background(), key)
	if err != nil {
		return err
	}

	J.m.Lock()
	defer J.m.Unlock()

	// Reset state
	J.entries = &sync.Map{}
	J.dropQueue = nil
	J.parquetSizeBytes = 0
	J.rowCount = 0
	J.minTime = 0
	J.maxTime = 0
	J.lastId = 0
	J.etag = etag

	if data != nil {
		err = J.parseMetadata(data)
		if err != nil {
			return err
		}
	}

	// Re-apply pending changes on top of remote state
	if len(pendingAdd) > 0 {
		jEntries, err := J.entry2JEntry(pendingAdd)
		if err != nil {
			return err
		}
		J.add(jEntries)
	}
	if len(pendingRm) > 0 {
		J.rm(pendingRm)
		J.addToDropQueue(pendingRm)
	}

	return nil
}

func (J *s3PartIndex) Run() {
	go func() {
		for {
			select {
			case <-J.updateCtx.Done():
				J.flush()
			case <-J.workCtx.Done():
				return
			}
		}
	}()
}

func (J *s3PartIndex) Stop() {
	J.stop()
}

func (J *s3PartIndex) jEntry2Entry(_e *jsonIndexEntry) *IndexEntry {
	return &_e.IndexEntry
}

func (J *s3PartIndex) Get(layer string, _path string) *IndexEntry {
	e, _ := J.entries.Load(_path)
	if e == nil {
		return nil
	}
	_e := e.(*jsonIndexEntry)
	return J.jEntry2Entry(_e)
}

func (J *s3PartIndex) s3Prefix() string {
	return path.Join(J.s3Layer.Config.Prefix, J.database, J.table) + "/"
}
