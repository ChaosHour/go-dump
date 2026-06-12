package load

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// LoadState persists which files have been successfully loaded, enabling --resume.
// Written atomically to <directory>/load-state.json after each successful file load.
type LoadState struct {
	StartTime      time.Time `json:"start_time"`
	CompletedFiles []string  `json:"completed_files"`

	mu    sync.Mutex
	path  string
	index map[string]bool
}

// NewLoadState reads an existing load-state.json from dir, or returns a fresh state.
func NewLoadState(dir string) (*LoadState, error) {
	path := dir + "/load-state.json"
	ls := &LoadState{
		path:  path,
		index: make(map[string]bool),
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			ls.StartTime = time.Now().UTC()
			return ls, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(data, ls); err != nil {
		return nil, err
	}
	for _, f := range ls.CompletedFiles {
		ls.index[f] = true
	}
	return ls, nil
}

// HasFile reports whether name was already successfully loaded.
func (ls *LoadState) HasFile(name string) bool {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.index[name]
}

// Len returns the number of files recorded as completed.
func (ls *LoadState) Len() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return len(ls.CompletedFiles)
}

// Mark records name as complete and flushes the state file atomically.
func (ls *LoadState) Mark(name string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.index[name] {
		return nil
	}
	ls.index[name] = true
	ls.CompletedFiles = append(ls.CompletedFiles, name)
	return ls.write()
}

func (ls *LoadState) write() error {
	data, err := json.MarshalIndent(ls, "", "  ")
	if err != nil {
		return err
	}
	tmp := ls.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, ls.path)
}
