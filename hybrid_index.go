package metadata

import (
	"fmt"
)

type HybridIndex struct {
	jsonIdx *JSONIndex
	s3Idx   *S3Index
	layers  []Layer
}

func NewIndex(root string, database string, table string, layers []Layer) (TableIndex, error) {
	var fsLayers, s3Layers []Layer
	for _, l := range layers {
		switch l.Type {
		case "fs":
			fsLayers = append(fsLayers, l)
		case "s3":
			s3Layers = append(s3Layers, l)
		default:
			return nil, fmt.Errorf("unsupported layer type: %q", l.Type)
		}
	}

	if len(s3Layers) == 0 {
		return NewJSONIndex(root, database, table, fsLayers)
	}
	if len(fsLayers) == 0 {
		return NewS3Index(database, table, s3Layers)
	}

	jsonIdx, err := NewJSONIndex(root, database, table, fsLayers)
	if err != nil {
		return nil, fmt.Errorf("create json index: %w", err)
	}
	s3Idx, err := NewS3Index(database, table, s3Layers)
	if err != nil {
		return nil, fmt.Errorf("create s3 index: %w", err)
	}

	return &HybridIndex{
		jsonIdx: jsonIdx.(*JSONIndex),
		s3Idx:   s3Idx.(*S3Index),
		layers:  layers,
	}, nil
}

func (h *HybridIndex) isS3Layer(layerName string) bool {
	for _, l := range h.layers {
		if l.Name == layerName {
			return l.Type == "s3"
		}
	}
	return false
}

func (h *HybridIndex) nextLayerName(currentLayer string) string {
	for i, l := range h.layers {
		if l.Name == currentLayer && i+1 < len(h.layers) {
			return h.layers[i+1].Name
		}
	}
	return ""
}

func (h *HybridIndex) Batch(add []*IndexEntry, rm []*IndexEntry) Promise[int32] {
	var jsonAdd, jsonRm, s3Add, s3Rm []*IndexEntry
	for _, e := range add {
		if h.isS3Layer(e.Layer) {
			s3Add = append(s3Add, e)
		} else {
			jsonAdd = append(jsonAdd, e)
		}
	}
	for _, e := range rm {
		if h.isS3Layer(e.Layer) {
			s3Rm = append(s3Rm, e)
		} else {
			jsonRm = append(jsonRm, e)
		}
	}

	var promises []Promise[int32]
	if len(jsonAdd) > 0 || len(jsonRm) > 0 {
		promises = append(promises, h.jsonIdx.Batch(jsonAdd, jsonRm))
	}
	if len(s3Add) > 0 || len(s3Rm) > 0 {
		promises = append(promises, h.s3Idx.Batch(s3Add, s3Rm))
	}
	if len(promises) == 0 {
		return Fulfilled[int32](nil, 0)
	}
	return NewWaitForAll[int32](promises)
}

func (h *HybridIndex) Get(layer string, _path string) *IndexEntry {
	if h.isS3Layer(layer) {
		return h.s3Idx.Get(layer, _path)
	}
	return h.jsonIdx.Get(layer, _path)
}

func (h *HybridIndex) Query(options QueryOptions) ([]*IndexEntry, error) {
	jsonEntries, err := h.jsonIdx.Query(options)
	if err != nil {
		return nil, err
	}
	s3Entries, err := h.s3Idx.Query(options)
	if err != nil {
		return nil, err
	}
	return append(jsonEntries, s3Entries...), nil
}

func (h *HybridIndex) GetAll() ([]*IndexEntry, error) {
	jsonEntries, err := h.jsonIdx.GetAll()
	if err != nil {
		return nil, err
	}
	s3Entries, err := h.s3Idx.GetAll()
	if err != nil {
		return nil, err
	}
	return append(jsonEntries, s3Entries...), nil
}

func (h *HybridIndex) Run() {
	h.jsonIdx.Run()
	h.s3Idx.Run()
}

func (h *HybridIndex) Stop() {
	h.jsonIdx.Stop()
	h.s3Idx.Stop()
}

func (h *HybridIndex) GetQuerier() TableQuerier {
	return h
}

func (h *HybridIndex) GetMergePlanner() TableMergePlanner {
	return h
}

func (h *HybridIndex) GetMergePlan(writerId string, layer string, iteration int) (MergePlan, error) {
	if h.isS3Layer(layer) {
		return h.s3Idx.GetMergePlan(writerId, layer, iteration)
	}
	return h.jsonIdx.GetMergePlan(writerId, layer, iteration)
}

func (h *HybridIndex) EndMerge(plan MergePlan) Promise[int32] {
	if h.isS3Layer(plan.Layer) {
		return h.s3Idx.EndMerge(plan)
	}
	return h.jsonIdx.EndMerge(plan)
}

func (h *HybridIndex) GetMovePlanner() TableMovePlanner {
	return h
}

func (h *HybridIndex) GetMovePlan(writerId string, layer string) (MovePlan, error) {
	if h.isS3Layer(layer) {
		return h.s3Idx.GetMovePlan(writerId, layer)
	}
	plan, err := h.jsonIdx.GetMovePlan(writerId, layer)
	if err != nil {
		return plan, err
	}
	if plan.PathFrom != "" && plan.LayerTo == "" {
		plan.LayerTo = h.nextLayerName(layer)
	}
	return plan, nil
}

func (h *HybridIndex) EndMove(plan MovePlan) Promise[int32] {
	if h.isS3Layer(plan.LayerFrom) {
		return h.s3Idx.EndMove(plan)
	}
	return h.jsonIdx.EndMove(plan)
}

func (h *HybridIndex) GetDropPlanner() TableDropPlanner {
	return h
}

func (h *HybridIndex) GetDropQueue(writerId string, layer string) (DropPlan, error) {
	if h.isS3Layer(layer) {
		return h.s3Idx.GetDropQueue(writerId, layer)
	}
	return h.jsonIdx.GetDropQueue(writerId, layer)
}

func (h *HybridIndex) RmFromDropQueue(plan DropPlan) Promise[int32] {
	if h.isS3Layer(plan.Layer) {
		return h.s3Idx.RmFromDropQueue(plan)
	}
	return h.jsonIdx.RmFromDropQueue(plan)
}
