package metadata

import (
	"context"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type S3Index struct {
	database string
	table    string
	lock     sync.Mutex
	parts    map[string]map[string]*s3PartIndex
	layers   []s3Layer
}

var dateHourRegex = regexp.MustCompile(`date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})`)

func NewS3Index(database string, table string, layers []Layer) (TableIndex, error) {
	var sLayers []s3Layer
	for _, layer := range layers {
		sl, err := newS3Layer(layer)
		if err != nil {
			return nil, err
		}
		sLayers = append(sLayers, sl)
	}
	res := &S3Index{
		database: database,
		table:    table,
		parts:    map[string]map[string]*s3PartIndex{},
		layers:   sLayers,
	}

	ctx := context.Background()
	for _, layer := range sLayers {
		prefix := path.Join(layer.Config.Prefix, database, table)
		metaFiles, err := layer.listMetadataFiles(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, metaFile := range metaFiles {
			// Extract partition path from: {prefix}/{db}/{table}/data/{partPath}/metadata.json
			trimmed := strings.TrimPrefix(metaFile, prefix+"/")
			partPath := strings.TrimSuffix(trimmed, "/metadata.json")
			if partPath == "" || partPath == trimmed {
				continue
			}
			_, err := res.populate(layer.Name, partPath)
			if err != nil {
				return nil, err
			}
		}
	}
	return res, nil
}

func (J *S3Index) GetQuerier() TableQuerier {
	return J
}

func (J *S3Index) GetAll() ([]*IndexEntry, error) {
	var res []*IndexEntry
	for _, l := range J.parts {
		for _, parts := range l {
			_res, err := parts.GetAll()
			if err != nil {
				return nil, err
			}
			res = append(res, _res...)
		}
	}
	return res, nil
}

func (J *S3Index) Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	J.lock.Lock()
	defer J.lock.Unlock()
	addByLayer := make(map[string][]*IndexEntry)
	rmByLayer := make(map[string][]*IndexEntry)
	layers := make(map[string]bool)
	for _, entry := range add {
		addByLayer[entry.Layer] = append(addByLayer[entry.Layer], entry)
		layers[entry.Layer] = true
	}
	for _, entry := range rm {
		rmByLayer[entry.Layer] = append(rmByLayer[entry.Layer], entry)
		layers[entry.Layer] = true
	}
	var promises []Promise[int32]
	for l := range layers {
		promises = append(promises, J.batchLayer(l, addByLayer[l], rmByLayer[l]))
	}
	return NewWaitForAll[int32](promises)
}

func (J *S3Index) batchLayer(layer string, add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	addByPath := make(map[string][]*IndexEntry)
	rmByPath := make(map[string][]*IndexEntry)
	paths := make(map[string]bool)
	for _, entry := range add {
		_path := path.Dir(entry.Path)
		addByPath[_path] = append(addByPath[_path], entry)
		paths[_path] = true
	}
	for _, entry := range rm {
		_path := path.Dir(entry.Path)
		rmByPath[_path] = append(rmByPath[_path], entry)
		paths[_path] = true
	}

	var promises []Promise[int32]
	for partPath := range paths {
		idx, err := J.populate(layer, partPath)
		if err != nil {
			return Fulfilled[int32](err, 0)
		}
		promises = append(promises, idx.Batch(addByPath[partPath], rmByPath[partPath]))
	}
	return NewWaitForAll[int32](promises)
}

func (J *S3Index) populate(layer string, dir string) (*s3PartIndex, error) {
	layerParts := J.parts[layer]
	if layerParts == nil {
		layerParts = make(map[string]*s3PartIndex)
		J.parts[layer] = layerParts
	}
	idx := layerParts[dir]
	var _layer *s3Layer
	for i := range J.layers {
		if J.layers[i].Name == layer {
			_layer = &J.layers[i]
			break
		}
	}
	if _layer == nil {
		return nil, fmt.Errorf("layer \"%s\" not found", layer)
	}

	if idx != nil {
		return idx, nil
	}
	idx, err := newS3PartIndex(s3PartIdxOpts{
		s3Layer:  _layer,
		database: J.database,
		table:    J.table,
		partPath: dir,
		layers:   J.layers,
		layer:    layer,
	})
	if err != nil {
		return nil, err
	}
	idx.Run()
	layerParts[dir] = idx
	return idx, nil
}

func (J *S3Index) Get(layer string, _path string) *IndexEntry {
	dir := path.Dir(_path)
	J.lock.Lock()
	defer J.lock.Unlock()
	idx, err := J.populate(layer, dir)
	if err != nil {
		return nil
	}
	return idx.Get(layer, _path)
}

func (J *S3Index) Run() {
}

func (J *S3Index) Stop() {
	for _, l := range J.parts {
		for _, idx := range l {
			idx.Stop()
		}
	}
}

func (J *S3Index) findHours(options QueryOptions, layer s3Layer) ([]time.Time, error) {
	var hours []time.Time
	ctx := context.Background()
	prefix := path.Join(layer.Config.Prefix, J.database, J.table)
	metaFiles, err := layer.listMetadataFiles(ctx, prefix)
	if err != nil {
		return nil, err
	}

	for _, metaFile := range metaFiles {
		matches := dateHourRegex.FindStringSubmatch(metaFile)
		if len(matches) != 3 {
			continue
		}
		date, err := time.Parse("2006-01-02", matches[1])
		if err != nil {
			continue
		}
		hour, err := strconv.Atoi(matches[2])
		if err != nil {
			continue
		}
		t := date.Add(time.Hour * time.Duration(hour))
		hours = append(hours, t)
	}

	var filtered []time.Time
	if options.Before.Unix() > 0 {
		for i := len(hours) - 1; i >= 0; i-- {
			if hours[i].Unix() < options.Before.Unix() {
				filtered = append(filtered, hours[i])
			}
		}
		hours = filtered
	}
	if options.After.Unix() > 0 {
		filtered = nil
		_after := time.Unix(options.After.Unix(), 0).Round(time.Hour)
		for _, hour := range hours {
			if hour.Unix() >= _after.Unix() {
				filtered = append(filtered, hour)
			}
		}
		hours = filtered
	}

	return hours, nil
}

func (J *S3Index) Query(options QueryOptions) ([]*IndexEntry, error) {
	var entries []*IndexEntry
	for _, l := range J.layers {
		hours, err := J.findHours(options, l)
		if err != nil {
			return nil, err
		}
		for _, hour := range hours {
			idx, err := J.populate(l.Name, path.Join(
				fmt.Sprintf("date=%s", hour.Format("2006-01-02")),
				fmt.Sprintf("hour=%02d", hour.Hour())))
			if err != nil {
				return nil, err
			}
			_entries, err := idx.Query(options)
			if err != nil {
				return nil, err
			}
			entries = append(entries, _entries...)
		}
	}
	return entries, nil
}
