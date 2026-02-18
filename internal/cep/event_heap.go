package cep

func (h eventHeap) Len() int { return len(h) }

func (h eventHeap) Less(i, j int) bool { return h[i].EventTime < h[j].EventTime }

func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x any) {
	*h = append(*h, x.(windowedEvent))
}

func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
