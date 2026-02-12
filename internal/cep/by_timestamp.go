package cep

func (a byTimestamp) Len() int { return len(a) }

func (a byTimestamp) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a byTimestamp) Less(i, j int) bool { return a[i].Timestamp < a[j].Timestamp }
