package logger

import (
	"os"
	"io"
	"log"
)


type Config struct {
	Stdout io.Writer
	Prefix string
	Flags  int
}

func NewLogger(cfg *Config) *log.Logger{
	if cfg == nil {
		return log.New(os.Stdout, "", log.Lmicroseconds)
	}
	return log.New(cfg.Stdout, cfg.Prefix, cfg.Flags)

}
