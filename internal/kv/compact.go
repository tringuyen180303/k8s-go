package kv

import "fmt"

// Compact drops MVCC versions that are no longer needed to serve reads at rev
// or later. The current map is unchanged. A later GetAt below rev returns
// nothing. This is revision history, separate from throwing away old WAL
// records in SaveSnapshot.
func (s *Store) Compact(rev uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rev > s.rev {
		return fmt.Errorf("kv: compact revision %d is past %d", rev, s.rev)
	}
	for key, versions := range s.history {
		keepFrom := 0
		for i, v := range versions {
			if v.revision <= rev {
				keepFrom = i
			}
		}
		s.history[key] = append([]version(nil), versions[keepFrom:]...)
	}
	if rev > s.compactRev {
		s.compactRev = rev
	}
	return nil
}
