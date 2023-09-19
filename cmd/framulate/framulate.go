package main

import (
	"context"
	"flag"
	"os"

	"github.com/nuclio/errors"
	nucliozap "github.com/nuclio/zap"
	"github.com/v3io/frames/framulate"
)

func run(configContents string, configPath string) error {
	loggerInstance, err := nucliozap.NewNuclioZapCmd("framulate", nucliozap.DebugLevel, os.Stdout)
	if err != nil {
		return errors.Wrap(err, "Failed to create logger")
	}

	config, err := framulate.NewConfigFromContentsOrPath([]byte(configContents), configPath)
	if err != nil {
		return errors.Wrap(err, "Failed to create config")
	}

	framulateInstance, err := framulate.NewFramulate(context.TODO(),
		loggerInstance,
		config)
	if err != nil {
		return errors.Wrap(err, "Failed to create framulate")
	}

	return framulateInstance.Start()
}

func main() {
	configPath := ""
	configContents := ""

	flag.StringVar(&configPath, "config-path", "", "")
	flag.StringVar(&configContents, "config-contents", "", "")

	flag.Parse()

	if err := run(configContents, configPath); err != nil {
		errors.PrintErrorStack(os.Stderr, err, 10)
		os.Exit(1)
	}
}
