package metadata

func (J *s3PartIndex) GetDropPlanner() TableDropPlanner {
	return J
}

func (J *s3PartIndex) RmFromDropQueue(plan DropPlan) Promise[int32] {
	J.m.Lock()
	defer J.m.Unlock()

	updated := false

	for i := len(J.dropQueue) - 1; i >= 0; i-- {
		if J.dropQueue[i].Path != plan.Path {
			continue
		}
		J.dropQueue[i] = J.dropQueue[len(J.dropQueue)-1]
		J.dropQueue = J.dropQueue[:len(J.dropQueue)-1]
		updated = true
		break
	}

	if !updated {
		return Fulfilled[int32](nil, 0)
	}

	p := NewPromise[int32]()
	J.promises = append(J.promises, p)
	J.doUpdate()
	return p
}

func (J *s3PartIndex) GetDropQueue(writerId string, layer string) (DropPlan, error) {
	if len(J.dropQueue) == 0 {
		return DropPlan{}, nil
	}
	return J.dropQueue[0], nil
}
