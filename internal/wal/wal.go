package wal

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// frame is one log record plus the CRC of its exact JSON bytes.
type frame struct {
	Sum  uint32          `json:"sum"`
	Body json.RawMessage `json:"body"`
}

const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
)

// Record is one durable operation. Append writes it before memory changes.
type Record struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Log is an append-only file of records.
type Log struct {
	mu sync.Mutex
	f  *os.File
}

// Open opens or creates the log file at path.
func Open(path string) (*Log, error) {
	// TODO: os.OpenFile with O_CREATE, O_RDWR, and O_APPEND.
	// O_APPEND makes every write land at the end of the file.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Log{f: f}, nil
}

// Append writes one record and fsyncs it.
// After Append returns nil, a crash must not lose this record.
func (l *Log) Append(rec Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	body, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	err = json.NewEncoder(l.f).Encode(frame{
		Sum:  crc32.ChecksumIEEE(body),
		Body: body,
	})
	if err != nil {
		return err
	}
	return l.f.Sync()
}

func (l *Log) Replay() ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(l.f)
	var records []Record
	for {
		var fr frame
		err := decoder.Decode(&fr)
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, err
		}
		if crc32.ChecksumIEEE(fr.Body) != fr.Sum {
			return nil, fmt.Errorf("wal: checksum mismatch")
		}
		var rec Record
		if err := json.Unmarshal(fr.Body, &rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
}

// Reset removes every record from the log file.
// The next Append starts a new log. Older state belongs in a snapshot.
func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.f.Truncate(0); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	_, err := l.f.Seek(0, io.SeekStart)
	return err
}

// Close closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// TODO: close l.f
	return l.f.Close()
}
