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
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
)

const (
	tagBase = "v3io/framesd"
)

func printCmd(prog string, args []string) {
	fmt.Print(prog)
	for _, arg := range args {
		if strings.Index(arg, " ") != -1 {
			arg = fmt.Sprintf("%q", arg)
		}
		fmt.Printf(" %s", arg)
	}
	fmt.Println()
}

func runOutput(prog string, args ...string) (string, error) {
	printCmd(prog, args)
	var buf bytes.Buffer
	cmd := exec.Command(prog, args...)
	cmd.Stdout = &buf
	if err := cmd.Run(); err != nil {
		return "", err
	}

	return buf.String(), nil
}

func run(prog string, args ...string) error {
	printCmd(prog, args)
	cmd := exec.Command(prog, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func buildVersion() string {
	tag := os.Getenv("TRAVIS_TAG")
	if tag != "" {
		return tag
	}

	tag = os.Getenv("TRAVIS_COMMIT")
	if tag != "" {
		return tag[:7]
	}

	out, err := runOutput("git", "rev-parse", "--short", "HEAD")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(out)
}

func buildDocker(version, tag string) error {
	return run(
		"docker", "build",
		"--build-arg", fmt.Sprintf("FRAMES_VERSION=%s", version),
		"--tag", tag,
		"--file", "cmd/framesd/Dockerfile",
		".",
	)
}

func tagFor(version string) string {
	return fmt.Sprintf("%s:%s", tagBase, version)
}

func gitBranch() string {
	out, err := runOutput("git", "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func dockerPush(tag string) error {
	return run("docker", "push", tag)
}

func docker() {
	version := buildVersion()
	tag := tagFor(version)
	fmt.Printf("building version %s\n", version)
	if err := buildDocker(version, tag); err != nil {
		log.Fatalf("error: can't build docker - %s", err)
	}

	alias := ""
	switch gitBranch() {
	case "master":
		alias = tagFor("latest")
	case "development":
		alias = tagFor("unstable")
	}

	if alias != "" {
		if err := run("docker", "tag", tag, alias); err != nil {
			log.Fatal("error: can't tag")
		}
	}

	user := os.Getenv("DOCKER_USERNAME")
	passwd := os.Getenv("DOCKER_PASSWORD")
	if user == "" || passwd == "" {
		fmt.Println("missing docker login info - exiting")
		return
	}

	err := run("docker", "login", "--username", user, "--password", passwd)
	if err != nil {
		log.Fatal("error: can't login to docker")
	}

	if err := dockerPush(tag); err != nil {
		log.Fatalf("error: can't push %s to docker", tag)
	}

	if alias != "" {
		if err := dockerPush(alias); err != nil {
			log.Fatalf("error: can't push %s to docker", alias)
		}
	}
}

func binaries() {
	defer func() {
		os.Unsetenv("GOOS")
		os.Unsetenv("GOARCH")
	}()

	version := buildVersion()
	os.Setenv("GOARCH", "amd64")
	for _, goos := range []string{"linux", "darwin", "windows"} {
		exe := fmt.Sprintf("framesd-%s-amd64", goos)
		if goos == "windows" {
			exe += ".exe"
		}
		ldFlags := fmt.Sprintf("-X main.Version=%s", version)

		os.Setenv("GOOS", goos)
		err := run(
			"go", "build",
			"-o", exe,
			"-ldflags", ldFlags,
			"./cmd/framesd",
		)
		if err != nil {
			log.Fatalf("error: can't build for %s", goos)
		}
	}
}

func main() {
	log.SetFlags(0) // Remove time prefix

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s docker|binaries\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()

	if flag.NArg() != 1 {
		log.Fatal("error: wrong number of arguments")
	}

	switch action := flag.Arg(0); action {
	case "docker":
		docker()
	case "binaries":
		binaries()
	default:
		log.Fatalf("error: unknown action - %s", action)
	}
}
