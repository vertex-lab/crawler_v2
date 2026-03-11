package recorder

import (
	"errors"
	"fmt"
)

type Config struct {
	// Queue represents the capacity of the recorder events queue. Default is 10k.
	Queue int `env:"RECORDER_QUEUE"`
}

func NewConfig() Config {
	return Config{
		Queue: 10_000,
	}
}

func (c Config) Validate() error {
	if c.Queue <= 0 {
		return errors.New("queue value must be non-negative")
	}
	return nil
}

func (c Config) String() string {
	return fmt.Sprintf("Recorder\n"+
		"\tQueue: %d\n",
		c.Queue)
}
