package cep

func (w WindowType) String() string {
	switch w {
	case WindowTumbling:
		return "tumbling"
	case WindowSliding:
		return "sliding"
	case WindowSession:
		return "session"
	case WindowCount:
		return "count"
	default:
		return "unknown"
	}
}
