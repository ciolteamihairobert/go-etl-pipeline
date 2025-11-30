package logger

import (
	"io"
	"log"
	"os"
)

var Info *log.Logger  // logger pentru mesaje info
var Error *log.Logger // logger pentru mesaje de eroare

func Init() {
	logFile, err := os.OpenFile("pipeline.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666) // deschidem fisierul de log
	if err != nil {                                                                        // daca apare o eroare la deschidere
		log.Fatalf("failed to open log file: %v", err) // logam eroarea si oprim executia
	}

	multiInfo := io.MultiWriter(os.Stdout, logFile)  // scriem atat in stdout cat si in fisier
	multiError := io.MultiWriter(os.Stderr, logFile) // scriem atat in stderr cat si in fisier

	Info = log.New(multiInfo, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)    // initializam logger-ul Info
	Error = log.New(multiError, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile) // initializam logger-ul Error
}
