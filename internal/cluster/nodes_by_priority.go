package cluster

func (n nodesByPriority) Len() int { return len(n) }

func (n nodesByPriority) Swap(i, j int) { n[i], n[j] = n[j], n[i] }

func (n nodesByPriority) Less(i, j int) bool { return n[i].Term < n[j].Term }
