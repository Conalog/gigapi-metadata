package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type s3KVStoreIndex struct {
	s3Layer *s3Layer
	key     string

	cache        map[string][]byte
	etag         string
	saveCtx      context.Context
	planSave     func()
	ctx          context.Context
	destroy      func()
	m            sync.Mutex
	savePromises []Promise[int32]

	pendingPuts    map[string][]byte
	pendingDeletes map[string]bool
}

func NewS3KVStoreIndex(s3URL string) (KVStoreIndex, error) {
	layer := Layer{URL: s3URL, Type: "s3"}
	sl, err := newS3Layer(layer)
	if err != nil {
		return nil, err
	}

	j := &s3KVStoreIndex{
		s3Layer:        &sl,
		key:            sl.kvObjectPath(),
		cache:          make(map[string][]byte),
		pendingPuts:    make(map[string][]byte),
		pendingDeletes: make(map[string]bool),
	}
	j.saveCtx, j.planSave = context.WithCancel(context.Background())
	j.ctx, j.destroy = context.WithCancel(context.Background())

	err = j.load()
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case <-j.saveCtx.Done():
				j.doSave()
			case <-j.ctx.Done():
				return
			}
		}
	}()

	return j, nil
}

func (j *s3KVStoreIndex) load() error {
	data, etag, err := j.s3Layer.getObject(context.Background(), j.key)
	if err != nil {
		return err
	}
	if data == nil {
		// Object doesn't exist yet, start with empty cache
		return nil
	}
	j.etag = etag
	return json.Unmarshal(data, &j.cache)
}

func (j *s3KVStoreIndex) Destroy() {
	j.destroy()
}

func (j *s3KVStoreIndex) Get(key string) ([]byte, error) {
	j.m.Lock()
	defer j.m.Unlock()

	if val, ok := j.cache[key]; ok {
		return val, nil
	}
	return nil, nil
}

func (j *s3KVStoreIndex) Put(key string, value []byte) error {
	j.m.Lock()
	j.cache[key] = value
	j.pendingPuts[key] = value
	delete(j.pendingDeletes, key)
	p := NewPromise[int32]()
	j.savePromises = append(j.savePromises, p)
	j.planSave()
	j.m.Unlock()
	_, err := p.Get()
	return err
}

func (j *s3KVStoreIndex) Delete(key string) error {
	j.m.Lock()
	delete(j.cache, key)
	j.pendingDeletes[key] = true
	delete(j.pendingPuts, key)
	p := NewPromise[int32]()
	j.savePromises = append(j.savePromises, p)
	j.planSave()
	j.m.Unlock()
	_, err := p.Get()
	return err
}

const maxKVFlushRetries = 3

func (j *s3KVStoreIndex) doSave() {
	j.m.Lock()
	cache := j.cache
	promises := j.savePromises
	j.savePromises = nil
	j.saveCtx, j.planSave = context.WithCancel(context.Background())
	pendingPuts := j.pendingPuts
	pendingDeletes := j.pendingDeletes
	j.pendingPuts = make(map[string][]byte)
	j.pendingDeletes = make(map[string]bool)
	j.m.Unlock()

	release := func(err error) {
		for _, p := range promises {
			p.Done(0, err)
		}
	}

	for attempt := 0; attempt < maxKVFlushRetries; attempt++ {
		contents, err := json.Marshal(cache)
		if err != nil {
			release(err)
			return
		}

		newETag, err := j.s3Layer.checkAndPut(context.Background(), j.key, contents, j.etag)
		if err == nil {
			j.m.Lock()
			j.etag = newETag
			j.m.Unlock()
			release(nil)
			return
		}

		if err != errETagMismatch {
			release(err)
			return
		}

		// ETag mismatch: re-read, merge, retry
		err = j.reloadAndMerge(pendingPuts, pendingDeletes)
		if err != nil {
			release(err)
			return
		}

		// Update cache reference after merge
		j.m.Lock()
		cache = j.cache
		j.m.Unlock()
	}

	release(fmt.Errorf("s3: kv store flush failed after %d retries due to concurrent modifications", maxKVFlushRetries))
}

func (j *s3KVStoreIndex) reloadAndMerge(pendingPuts map[string][]byte, pendingDeletes map[string]bool) error {
	data, etag, err := j.s3Layer.getObject(context.Background(), j.key)
	if err != nil {
		return err
	}

	j.m.Lock()
	defer j.m.Unlock()

	j.etag = etag
	j.cache = make(map[string][]byte)

	if data != nil {
		err = json.Unmarshal(data, &j.cache)
		if err != nil {
			return err
		}
	}

	// Re-apply pending changes
	for k, v := range pendingPuts {
		j.cache[k] = v
	}
	for k := range pendingDeletes {
		delete(j.cache, k)
	}

	return nil
}
