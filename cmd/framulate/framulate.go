/*
Copyright 2018 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/
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
	loggerInstance, err := nucliozap.NewNuclioZapCmd("framulate", nucliozap.DebugLevel)
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
