package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

const maxQueryBufferSize = 10000

// QueryRecord holds information about a single SQL query that passed through the proxy.
type QueryRecord struct {
	ID          uint64    `json:"id"`
	Time        time.Time `json:"time"`
	ClientInfo  string    `json:"client"`
	OriginalSQL string    `json:"original"`
	FinalSQL    string    `json:"final"`       // empty when not transformed
	TranslateUs int64     `json:"translateUs"` // translation duration in microseconds
	Transformed bool      `json:"transformed"`
}

// QueryStore is a thread-safe fixed-size ring buffer of QueryRecords with SSE subscriber support.
// Memory is bounded permanently to maxQueryBufferSize entries.
type QueryStore struct {
	mu          sync.RWMutex
	entries     [maxQueryBufferSize]QueryRecord
	head        int // next slot to write
	count       int // number of valid entries (0..maxQueryBufferSize)
	nextID      uint64
	subscribers map[uint64]chan QueryRecord
	nextSubID   uint64
}

func NewQueryStore() *QueryStore {
	return &QueryStore{
		subscribers: make(map[uint64]chan QueryRecord),
	}
}

// Add writes a record into the ring buffer (overwriting the oldest when full)
// and broadcasts to all SSE subscribers.
func (s *QueryStore) Add(r QueryRecord) {
	s.mu.Lock()
	r.ID = s.nextID
	s.nextID++
	s.entries[s.head] = r
	s.head = (s.head + 1) % maxQueryBufferSize
	if s.count < maxQueryBufferSize {
		s.count++
	}
	// Broadcast to subscribers (non-blocking)
	for _, ch := range s.subscribers {
		select {
		case ch <- r:
		default:
		}
	}
	s.mu.Unlock()
}

// Recent returns a copy of all buffered records in insertion order (oldest first).
func (s *QueryStore) Recent() []QueryRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]QueryRecord, s.count)
	start := (s.head - s.count + maxQueryBufferSize) % maxQueryBufferSize
	for i := range out {
		out[i] = s.entries[(start+i)%maxQueryBufferSize]
	}
	return out
}

// Subscribe registers a new SSE subscriber. Returns the sub ID and a receive channel.
func (s *QueryStore) Subscribe() (uint64, <-chan QueryRecord) {
	ch := make(chan QueryRecord, 64)
	s.mu.Lock()
	id := s.nextSubID
	s.nextSubID++
	s.subscribers[id] = ch
	s.mu.Unlock()
	return id, ch
}

// Unsubscribe removes a subscriber.
func (s *QueryStore) Unsubscribe(id uint64) {
	s.mu.Lock()
	if ch, ok := s.subscribers[id]; ok {
		delete(s.subscribers, id)
		close(ch)
	}
	s.mu.Unlock()
}

// StartWebServer starts the HTTP server for the web UI in a goroutine.
func StartWebServer(port int, store *QueryStore) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, webUIHTML)
	})
	mux.HandleFunc("/queries", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(store.Recent())
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "Streaming not supported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Send existing records as backfill
		for _, rec := range store.Recent() {
			data, _ := json.Marshal(rec)
			fmt.Fprintf(w, "data: %s\n\n", data)
		}
		flusher.Flush()

		subID, ch := store.Subscribe()
		defer store.Unsubscribe(subID)

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case rec, ok := <-ch:
				if !ok {
					return
				}
				data, _ := json.Marshal(rec)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	})

	go func() {
		addr := fmt.Sprintf(":%d", port)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("Web UI server error: %v", err)
		}
	}()
}

const webUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>pg-proxy — Query Monitor</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --border: #2a2d3a;
    --accent: #4f8ef7;
    --accent-dim: #2a3f6f;
    --text: #c8ccd8;
    --text-dim: #606478;
    --green: #3ecf6e;
    --yellow: #f7b84f;
    --red: #f75f5f;
    --mono: 'SFMono-Regular', 'Consolas', 'Liberation Mono', monospace;
  }
  body { background: var(--bg); color: var(--text); font-family: var(--mono); font-size: 13px; height: 100vh; display: flex; flex-direction: column; overflow: hidden; }
  header { padding: 10px 16px; background: var(--surface); border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 12px; flex-shrink: 0; }
  header h1 { font-size: 14px; font-weight: 600; color: var(--accent); letter-spacing: 0.05em; margin-right: auto; }
  #status { font-size: 11px; color: var(--text-dim); }
  #status.connected::before { content: '● '; color: var(--green); }
  #status.disconnected::before { content: '● '; color: var(--red); }
  #filter { background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: 4px 10px; border-radius: 4px; font-family: var(--mono); font-size: 12px; width: 240px; outline: none; }
  #filter:focus { border-color: var(--accent); }
  button { background: var(--surface); border: 1px solid var(--border); color: var(--text); padding: 4px 12px; border-radius: 4px; cursor: pointer; font-family: var(--mono); font-size: 12px; }
  button:hover { border-color: var(--accent); color: var(--accent); }
  button.active { background: var(--accent-dim); border-color: var(--accent); color: var(--accent); }
  #counter { font-size: 11px; color: var(--text-dim); min-width: 80px; text-align: right; }
  .table-wrap { flex: 1; overflow-y: auto; }
  table { width: 100%; border-collapse: collapse; table-layout: fixed; }
  colgroup col:nth-child(1) { width: 80px; }
  colgroup col:nth-child(2) { width: 160px; }
  colgroup col:nth-child(3) { width: calc(50% - 200px); }
  colgroup col:nth-child(4) { width: calc(50% - 80px); }
  colgroup col:nth-child(5) { width: 80px; }
  thead th { position: sticky; top: 0; background: var(--surface); border-bottom: 1px solid var(--border); padding: 7px 10px; text-align: left; font-size: 11px; color: var(--text-dim); font-weight: 500; letter-spacing: 0.06em; text-transform: uppercase; z-index: 1; }
  tbody tr { border-bottom: 1px solid var(--border); transition: background 0.1s; }
  tbody tr:hover { background: var(--surface); }
  tbody tr.transformed { border-left: 2px solid var(--yellow); }
  tbody tr.new-row { animation: flash 0.4s ease-out; }
  @keyframes flash { from { background: var(--accent-dim); } to { background: transparent; } }
  td { padding: 6px 10px; vertical-align: top; overflow: hidden; white-space: pre-wrap; word-break: break-all; line-height: 1.5; }
  td.time { color: var(--text-dim); font-size: 11px; white-space: nowrap; }
  td.client { color: var(--text-dim); font-size: 11px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  td.sql { color: var(--text); }
  td.sql .translated { color: var(--yellow); }
  td.dur { color: var(--text-dim); font-size: 11px; text-align: right; white-space: nowrap; }
  td.dur.fast { color: var(--green); }
  #empty { text-align: center; color: var(--text-dim); padding: 60px; font-size: 14px; }
  mark { background: var(--accent-dim); color: var(--accent); border-radius: 2px; }
</style>
</head>
<body>
<header>
  <h1>pg-proxy &mdash; Query Monitor</h1>
  <span id="status" class="disconnected">connecting</span>
  <input id="filter" type="text" placeholder="Filter queries…" autocomplete="off" spellcheck="false">
  <button id="pauseBtn">Pause</button>
  <button id="clearBtn">Clear</button>
  <span id="counter">0 queries</span>
</header>
<div class="table-wrap" id="tableWrap">
  <table id="queryTable">
    <colgroup>
      <col><col><col><col><col>
    </colgroup>
    <thead>
      <tr>
        <th>Time</th>
        <th>Client</th>
        <th>Original SQL</th>
        <th>Translated SQL</th>
        <th>Duration</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  <div id="empty">Waiting for queries…</div>
</div>
<script>
(function() {
  const MAX_ROWS = 500;
  const tbody = document.getElementById('tbody');
  const empty = document.getElementById('empty');
  const counter = document.getElementById('counter');
  const filterInput = document.getElementById('filter');
  const pauseBtn = document.getElementById('pauseBtn');
  const clearBtn = document.getElementById('clearBtn');
  const tableWrap = document.getElementById('tableWrap');
  const statusEl = document.getElementById('status');

  let paused = false;
  let filterText = '';
  let totalCount = 0;

  function formatTime(iso) {
    const d = new Date(iso);
    return d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0');
  }

  function formatDur(us) {
    if (us === 0) return '';
    if (us < 1000) return us + ' µs';
    if (us < 1000000) return (us / 1000).toFixed(1) + ' ms';
    return (us / 1000000).toFixed(2) + ' s';
  }

  function esc(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function highlight(str, term) {
    if (!term) return esc(str);
    const idx = str.toLowerCase().indexOf(term.toLowerCase());
    if (idx === -1) return esc(str);
    return esc(str.slice(0, idx)) + '<mark>' + esc(str.slice(idx, idx + term.length)) + '</mark>' + highlight(str.slice(idx + term.length), term);
  }

  function rowMatchesFilter(rec) {
    if (!filterText) return true;
    const t = filterText.toLowerCase();
    return rec.original.toLowerCase().includes(t) || rec.final.toLowerCase().includes(t) || rec.client.toLowerCase().includes(t);
  }

  function makeRow(rec, animate) {
    const tr = document.createElement('tr');
    if (rec.transformed) tr.classList.add('transformed');
    if (animate) tr.classList.add('new-row');
    tr.dataset.id = rec.id;
    tr.dataset.original = rec.original;
    tr.dataset.final = rec.final;
    tr.dataset.client = rec.client;

    const durClass = rec.translateUs > 0 && rec.translateUs < 5000 ? 'dur fast' : 'dur';

    tr.innerHTML =
      '<td class="time">' + formatTime(rec.time) + '</td>' +
      '<td class="client" title="' + esc(rec.client) + '">' + esc(rec.client) + '</td>' +
      '<td class="sql">' + highlight(rec.original, filterText) + '</td>' +
      '<td class="sql translated">' + (rec.transformed ? highlight(rec.final, filterText) : '<span style="color:var(--text-dim)">—</span>') + '</td>' +
      '<td class="' + durClass + '">' + (rec.transformed ? formatDur(rec.translateUs) : '') + '</td>';
    return tr;
  }

  function applyFilter() {
    let visible = 0;
    for (const tr of tbody.rows) {
      const rec = { original: tr.dataset.original, final: tr.dataset.final, client: tr.dataset.client };
      const show = rowMatchesFilter(rec);
      tr.style.display = show ? '' : 'none';
      if (show) visible++;
      // Re-highlight
      if (show && filterText) {
        tr.cells[2].innerHTML = highlight(rec.original, filterText);
        tr.cells[3].innerHTML = rec.final
          ? highlight(rec.final, filterText)
          : '<span style="color:var(--text-dim)">—</span>';
      }
    }
    counter.textContent = totalCount + ' queries' + (filterText ? ' (' + visible + ' shown)' : '');
  }

  function addRecord(rec, animate) {
    totalCount++;
    empty.style.display = 'none';

    // Prune oldest rows
    while (tbody.rows.length >= MAX_ROWS) {
      tbody.deleteRow(0);
    }

    const tr = makeRow(rec, animate);
    const show = rowMatchesFilter(rec);
    tr.style.display = show ? '' : 'none';
    tbody.appendChild(tr);

    counter.textContent = totalCount + ' queries';

    if (!paused && show) {
      tr.scrollIntoView({ block: 'end', behavior: 'smooth' });
    }
  }

  // Pause button
  pauseBtn.addEventListener('click', function() {
    paused = !paused;
    pauseBtn.textContent = paused ? 'Resume' : 'Pause';
    pauseBtn.classList.toggle('active', paused);
  });

  // Clear button
  clearBtn.addEventListener('click', function() {
    tbody.innerHTML = '';
    totalCount = 0;
    counter.textContent = '0 queries';
    empty.style.display = '';
  });

  // Filter
  filterInput.addEventListener('input', function() {
    filterText = this.value.trim();
    applyFilter();
  });

  // SSE connection
  function connect() {
    const es = new EventSource('/events');
    statusEl.className = 'disconnected';
    statusEl.textContent = 'connecting';

    es.onopen = function() {
      statusEl.className = 'connected';
      statusEl.textContent = 'live';
    };

    es.onmessage = function(e) {
      try {
        const rec = JSON.parse(e.data);
        addRecord(rec, true);
      } catch(err) {}
    };

    es.onerror = function() {
      statusEl.className = 'disconnected';
      statusEl.textContent = 'reconnecting…';
      es.close();
      setTimeout(connect, 3000);
    };
  }

  connect();
})();
</script>
</body>
</html>`
