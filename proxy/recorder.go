package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Interaction struct {
	Type          string    `json:"type"` // "simple" | "extended"
	OriginalSQL   string    `json:"original_sql"`
	StatementName string    `json:"statement_name,omitempty"`
	Parameters    []string  `json:"parameters,omitempty"`
	Time          time.Time `json:"time"`
}

type Recording struct {
	ClientInfo        string            `json:"client_info"`
	StartupParameters map[string]string `json:"startup_parameters"`
	Interactions      []Interaction     `json:"interactions"`
}

type Recorder struct {
	mu        sync.Mutex
	recording Recording
	dir       string
	filename  string
}

func NewRecorder(dir string) *Recorder {
	return &Recorder{dir: dir}
}

func (r *Recorder) SetMeta(clientInfo string, pid uint32, params map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ts := time.Now().UTC().Format("2006-01-02T15-04-05Z")
	r.filename = filepath.Join(r.dir, fmt.Sprintf("record-%s-%d.json", ts, pid))
	r.recording.ClientInfo = clientInfo
	r.recording.StartupParameters = params
}

func (r *Recorder) AddSimple(sql string) {
	if r == nil || r.filename == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recording.Interactions = append(r.recording.Interactions, Interaction{
		Type:        "simple",
		OriginalSQL: sql,
		Time:        time.Now().UTC(),
	})
}

func (r *Recorder) AddExtended(sql, stmtName string, params []string) {
	if r == nil || r.filename == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recording.Interactions = append(r.recording.Interactions, Interaction{
		Type:          "extended",
		OriginalSQL:   sql,
		StatementName: stmtName,
		Parameters:    params,
		Time:          time.Now().UTC(),
	})
}

func (r *Recorder) Close() error {
	if r == nil || r.filename == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.recording.Interactions) == 0 {
		return nil
	}
	data, err := json.MarshalIndent(r.recording, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(r.filename, data, 0644)
}
