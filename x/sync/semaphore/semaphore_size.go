package semaphore

func (s *Weighted) SetSize(newSize int64) {
	s.mu.Lock()
	s.size = newSize
	s.notifyWaiters()
	s.mu.Unlock()
}
