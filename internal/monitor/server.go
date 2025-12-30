package monitor

import (
	"encoding/json"
	"net/http"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func StartServer(addr string) { // functie pentru a porni serverul HTTP de monitorizare
	mux := http.NewServeMux() // cream un nou multiplexer pentru rute

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { // endpoint pentru verificarea starii de sanatate
		w.WriteHeader(http.StatusOK) // setam codul de stare HTTP 200
		w.Write([]byte("ok"))        // raspundem cu "ok"
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) { // endpoint pentru obtinerea metrics
		w.Header().Set("Content-Type", "application/json") // setam header-ul Content-Type la application/json
		_ = json.NewEncoder(w).Encode(GetMetrics())        // codificam metrics in JSON si le trimitem ca raspuns
	})

	mux.HandleFunc("/history", func(w http.ResponseWriter, r *http.Request) { // endpoint pentru obtinerea istoricului rularilor
		w.Header().Set("Content-Type", "application/json") // setam header-ul Content-Type la application/json
		_ = json.NewEncoder(w).Encode(GetHistory())        // codificam istoricul in JSON si il trimitem ca raspuns
	})

	logger.Info.Printf("Monitoring HTTP server started on %s", addr) // logam mesajul de pornire a serverului

	if err := http.ListenAndServe(addr, mux); err != nil { // pornim serverul HTTP
		logger.Error.Printf("Monitoring server failed: %v", err) // logam eroarea daca serverul esueaza
	}
}
