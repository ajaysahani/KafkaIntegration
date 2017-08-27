package main

import (
	"fmt"
	"io"
	"os"

	log "github.com/Sirupsen/logrus"
)

var (
	// Log used for logging through app
	Log *Logrus
	// GetWriter gets output
	GetWriter = func(config Configuration) (io.Writer, error) {
		f, err := os.OpenFile(config.LogFile, os.O_WRONLY|os.O_CREATE, 0755)
		if err != nil {
			return nil, fmt.Errorf("Unable to open log file - %s, switching to standart output", config.LogFile)
		}
		return f, nil
	}
)

// Logrus used for logging
type Logrus struct {
	L *log.Logger
}

// LoadLogger loads  logger
func LoadLogger(config Configuration) (err error) {
	Log = &Logrus{
		L: &log.Logger{},
	}
	out, err := GetWriter(config)
	if err != nil {
		return err
	}
	Log.L.Out = out
	Log.L.Info("Log initialized")
	return nil
}
