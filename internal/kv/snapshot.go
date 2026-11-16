package kv

import (
	"encoding/json"
	"os"
)

type snapshotFile struct {
	Revision uint64            `json:"revision"`
	Data     map[string]string `json:"data"`
}

// SaveSnapshot writes the current keys and revision to path+".snap",
// then clears the log. A later Open loads that file and replays only
// the records appended after the snapshot.
func (s *Store) SaveSnapshot() error {
	s.mu.RLock()
	rev := s.rev
	copied := make(map[string]string, len(s.data))
	for k, v := range s.data {
		copied[k] = v
	}
	s.mu.RUnlock()

	f, err := os.Create(s.path + ".snap")
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(snapshotFile{Revision: rev, Data: copied}); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return s.log.Reset()
}

// applySnapshot loads path+".snap" when that file exists.
// A missing file is success and leaves the store empty.
func (s *Store) applySnapshot() error {
	f, err := os.Open(s.path + ".snap")
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	var snap snapshotFile
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		return err
	}
	s.rev = snap.Revision
	s.data = snap.Data
	return nil
}
