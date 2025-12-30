package monitor

import (
	"encoding/json"
	"net/http"

	"github.com/ciolteamihairobert/go-etl-pipeline/internal/logger"
)

func StartHTTPServer(addr string) { // functie pentru a porni serverul HTTP de monitorizare
	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { // endpoint pentru verificarea starii de sanatate
		w.Write([]byte("OK")) // raspundem cu "OK"
	})

	http.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) { // endpoint pentru obtinerea metrics
		json.NewEncoder(w).Encode(GetMetrics()) // codificam metrics in JSON si le trimitem ca raspuns
	})

	logger.Info.Printf("Monitoring HTTP server started on %s", addr) // logam mesajul de pornire a serverului
	go http.ListenAndServe(addr, nil)                                // pornim serverul HTTP in mod concurent
}
