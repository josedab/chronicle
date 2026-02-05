package edgemesh

func (s *mdnsServer) Stop() {
	if !s.running.Swap(false) {
		return
	}
	s.cancel()
}
