package monitor

import (
	"sync"
	"time"
)

type Metrics struct { // structura pentru stocarea metrics
	TotalRuns   int       `json:"total_runs"`   // numar total de rulari
	SuccessRuns int       `json:"success_runs"` // numar de rulari reusite
	FailedRuns  int       `json:"failed_runs"`  // numar de rulari esuate
	LastRun     time.Time `json:"last_run"`     // timestamp-ul ultimei rulari
	LastStatus  string    `json:"last_status"`  // "success" / "failed"
}

type RunRecord struct { // structura pentru inregistrarea unei rulari
	Time      time.Time     `json:"time"`            // timestamp-ul rularii
	Status    string        `json:"status"`          // "success" / "failed"
	Duration  time.Duration `json:"duration"`        // durata rularii
	Error     string        `json:"error,omitempty"` // mesajul de eroare, daca exista
	RowsIn    int           `json:"rows_in"`         // numar de randuri extrase
	RowsOut   int           `json:"rows_out"`        // numar de randuri incarcate
	Pipeline  string        `json:"pipeline"`        // numele pipeline-ului
	Extractor string        `json:"extractor"`       // numele extractor-ului
	Loader    string        `json:"loader"`          // numele loader-ului
}

var ( // variabile globale pentru stocarea metrics si istoricului
	mu         sync.Mutex  // mutex pentru sincronizare
	metrics    Metrics     // structura pentru metrics
	history    []RunRecord // slice pentru istoricul rularilor
	maxHistory = 50        // numar maxim de inregistrari in istoric
)

func UpdateRun(rec RunRecord) { // functie pentru actualizarea metrics si istoricului cu o noua inregistrare
	mu.Lock()         // blocam mutex-ul pentru a preveni accesul concurent
	defer mu.Unlock() // deblocam mutex-ul la final

	metrics.TotalRuns++             // incrementam numarul total de rulari
	metrics.LastRun = rec.Time      // actualizam timestamp-ul ultimei rulari
	metrics.LastStatus = rec.Status // actualizam status-ul ultimei rulari

	if rec.Status == "success" { // daca rulara a fost reusita
		metrics.SuccessRuns++ // incrementam numarul de rulari reusite
	} else {
		metrics.FailedRuns++ // altfel, incrementam numarul de rulari esuate
	}

	history = append(history, rec) // adaugam inregistrarea la istoric
	if len(history) > maxHistory { // pastram doar ultimele maxHistory de inregistrari
		history = history[len(history)-maxHistory:] // taiem istoricul pentru a pastra doar ultimele maxHistory
	}
}

func GetMetrics() Metrics { // functie pentru obtinerea metrics
	mu.Lock()         // blocam mutex-ul pentru a preveni accesul concurent
	defer mu.Unlock() // deblocam mutex-ul la final
	return metrics    // returnam metrics
}

func GetHistory() []RunRecord { // functie pentru obtinerea istoricului rularilor
	mu.Lock()         // blocam mutex-ul pentru a preveni accesul concurent
	defer mu.Unlock() // deblocam mutex-ul la final

	out := make([]RunRecord, len(history)) // cream un nou slice cu aceeasi lungime ca istoricul
	copy(out, history)                     // copiem continutul istoricului in noul slice
	return out                             // returnam copia istoricului
}
