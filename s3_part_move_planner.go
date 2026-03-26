package metadata

import (
	"time"
)

func (J *s3PartIndex) GetMovePlan(writerId string, layer string) (MovePlan, error) {
	J.m.Lock()
	defer J.m.Unlock()
	var plan *MovePlan
	J.entries.Range(func(key, value any) bool {
		val := value.(*jsonIndexEntry)
		if J.filesInMerge[val.Path] {
			return true
		}
		layerIdx := J.getLayer(val.Layer)
		if layerIdx < 0 {
			return true
		}
		layerTo := ""
		if layerIdx+1 < len(J.layers) {
			layerTo = J.layers[layerIdx+1].Name
		}
		if J.layers[layerIdx].TTLSec > 0 &&
			time.Now().UnixNano()-val.ChunkTime >= int64(J.layers[layerIdx].TTLSec)*1000000000 {
			plan = &MovePlan{
				ID:        "",
				Database:  J.database,
				Table:     J.table,
				PathFrom:  val.Path,
				LayerFrom: val.Layer,
				PathTo:    val.Path,
				LayerTo:   layerTo,
			}
			return false
		}
		return true
	})
	if plan == nil {
		return MovePlan{}, nil
	}
	return *plan, nil
}

func (J *s3PartIndex) EndMove(plan MovePlan) Promise[int32] {
	J.m.Lock()
	defer J.m.Unlock()
	if _, ok := J.filesInMove[plan.PathFrom]; !ok {
		return Fulfilled[int32](nil, 0)
	}
	delete(J.filesInMove, plan.PathFrom)
	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *s3PartIndex) GetMovePlanner() TableMovePlanner {
	return J
}
