package load

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// LoadState persists which files have been successfully loaded, enabling --resume.
// Written atomically to <directory>/load-state.json after each successful file load.
// FileProgress tracks partially-loaded data files by the number of statements
// committed so far, so an interrupted load resumes mid-file instead of
// replaying rows that are already in the target.
type LoadState struct {
	StartTime      time.Time        `json:"start_time"`
	CompletedFiles []string         `json:"completed_files"`
	FileProgress   map[string]int64 `json:"file_progress,omitempty"`

	mu    sync.Mutex
	path  string
	index map[string]bool
}

// NewLoadState reads an existing load-state.json from dir, or returns a fresh state.
func NewLoadState(dir string) (*LoadState, error) {
	path := dir + "/load-state.json"
	ls := &LoadState{
		path:         path,
		index:        make(map[string]bool),
		FileProgress: make(map[string]int64),
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
	if ls.FileProgress == nil {
		ls.FileProgress = make(map[string]int64)
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

// Progress returns the number of statements already committed for a
// partially-loaded file, or 0 when the file has not been started.
func (ls *LoadState) Progress(name string) int64 {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.FileProgress[name]
}

// SetProgress records that n statements of name have been committed and
// flushes the state file atomically.
func (ls *LoadState) SetProgress(name string, n int64) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.FileProgress[name] = n
	return ls.write()
}

// Mark records name as complete and flushes the state file atomically.
// Any partial progress entry for name is dropped — completed files are
// tracked by CompletedFiles alone.
func (ls *LoadState) Mark(name string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.index[name] {
		return nil
	}
	ls.index[name] = true
	ls.CompletedFiles = append(ls.CompletedFiles, name)
	delete(ls.FileProgress, name)
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
