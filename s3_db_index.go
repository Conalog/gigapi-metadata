package metadata

import (
	"context"
	"path"
	"strings"
)

type s3DBIndex struct {
	layers []s3Layer
}

func NewS3DBIndex(layers []Layer) (DBIndex, error) {
	sLayers := make([]s3Layer, 0, len(layers))
	for _, layer := range layers {
		sl, err := newS3Layer(layer)
		if err != nil {
			return nil, err
		}
		sLayers = append(sLayers, sl)
	}
	return &s3DBIndex{
		layers: sLayers,
	}, nil
}

func (j *s3DBIndex) Databases() ([]string, error) {
	res := map[string]bool{}
	ctx := context.Background()
	for _, l := range j.layers {
		names, err := l.listPrefixes(ctx, l.Config.Prefix)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			res[name] = true
		}
	}
	_res := make([]string, 0, len(res))
	for k := range res {
		_res = append(_res, k)
	}
	return _res, nil
}

func (j *s3DBIndex) Tables(database string) ([]string, error) {
	res := map[string]bool{}
	ctx := context.Background()
	for _, l := range j.layers {
		prefix := path.Join(l.Config.Prefix, database)
		names, err := l.listPrefixes(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, name := range names {
			res[name] = true
		}
	}
	_res := make([]string, 0, len(res))
	for k := range res {
		_res = append(_res, k)
	}
	return _res, nil
}

func (j *s3DBIndex) Paths(database string, table string) ([]string, error) {
	res := map[string]bool{}
	ctx := context.Background()
	for _, l := range j.layers {
		prefix := path.Join(l.Config.Prefix, database, table)
		metaFiles, err := l.listMetadataFiles(ctx, prefix)
		if err != nil {
			return nil, err
		}
		for _, metaFile := range metaFiles {
			// Extract partition path: date=YYYY-MM-DD/hour=HH
			trimmed := strings.TrimPrefix(metaFile, prefix+"/")
			partPath := strings.TrimSuffix(trimmed, "/metadata.json")
			if partPath != "" && partPath != trimmed {
				res[partPath] = true
			}
		}
	}
	_res := make([]string, 0, len(res))
	for k := range res {
		_res = append(_res, k)
	}
	return _res, nil
}
