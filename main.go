// Copyright 2015 Daniel Theophanes.
// Use of this source code is governed by a zlib-style
// license that can be found in the LICENSE file.

// Simple service that only works by printing a log message every few seconds.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"github.com/fsnotify/fsnotify"
	"github.com/kardianos/service"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

type thread struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
}

type config struct {
	Threads []thread `json:"threads"`
}

func checkExistence(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

func loadConfig(path string) (*config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	cf := &config{}
	jde := json.NewDecoder(f)

	err = jde.Decode(cf)
	if err != nil {
		return nil, err
	}

	return cf, nil
}

func copyFile(from string, to string) error {
	exist, err := checkExistence(to)
	if err != nil {
		logger.Errorf("Cannot check the destination folder existence: %s", to)
		return err
	}
	if !exist {
		logger.Warningf("Destination folder %s does not exist, it would be created.", to)
		return err
	}

	toFileName := filepath.Join(to, filepath.Base(from))
	exist, err = checkExistence(toFileName)
	if err != nil {
		logger.Errorf("Cannot check the destination file existence: %s", toFileName)
		return err
	}
	if exist {
		logger.Warningf("The destination file %s already exists.", toFileName)
		return nil
	}

	source, err := os.Open(from)
	if err != nil {
		logger.Errorf("Cannot open source file %s.", source)
		return err
	}
	defer source.Close()

	destination, err := os.Create(toFileName)
	if err != nil {
		logger.Errorf("Cannot create destination file %s.", destination)
		return err
	}
	defer destination.Close()

	nBytes, err := io.Copy(destination, source)
	if err != nil {
		logger.Errorf("Copy file %s failed.", source)
		return err
	}

	logger.Infof("Successfully copy file %s with %n bytes", from, nBytes)
	return nil
}

func matchThread(path string, confThreads []thread) int {

	for idx, th := range confThreads {
		if match, _ := filepath.Match(filepath.ToSlash(th.Source)+"/*", filepath.ToSlash(path)); match {
			return idx
		}
	}

	return -1
}

var logger service.Logger

type program struct {
	conf *config
	exit chan struct{}
}

func (p *program) Start(s service.Service) error {
	if service.Interactive() {
		logger.Info("Running in terminal.")
	} else {
		logger.Info("Running under service manager.")
	}
	p.exit = make(chan struct{})

	// Start should not block. Do the actual work async.
	go p.run()
	return nil
}

func (p *program) run() error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Error("Error on starting a watcher.")
		return err
	}
	defer watcher.Close()

	for _, th := range p.conf.Threads {
		err := watcher.Add(th.Source)
		if err != nil {
			logger.Errorf("Error on adding new file path %s", th.Source)
			return err
		}
	}

	logger.Infof("Start to listen on specified addresses.")
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Has(fsnotify.Create) {
				logger.Infof("New file created: %s", event.Name)
				idx := matchThread(event.Name, p.conf.Threads)
				if idx < 0 {
					logger.Errorf("New file %s fails to match any source paths specified.", event.Name)
					continue
				}

				time.Sleep(500 * time.Millisecond)

				err = copyFile(event.Name, p.conf.Threads[idx].Destination)
				if err != nil {
					logger.Errorf("Failed to copy file from %s to %s.", event.Name, p.conf.Threads[idx].Destination)
					continue
				}
				logger.Infof("Success to copy file from %s to %s.", event.Name, p.conf.Threads[idx].Destination)
			}

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			logger.Error(err)

		case <-p.exit:
			return nil
		}
	}
}

func (p *program) Stop(s service.Service) error {
	// Any work in Stop should be quick, usually a few seconds at most.
	logger.Info("Service shutting down ...")
	close(p.exit)
	return nil
}

func main() {
	svcFlag := flag.String("config", "", "Specify the config file.")
	flag.Parse()

	if len(*svcFlag) <= 0 {
		log.Fatal("No config file specified.")
		return
	}

	exist, err := checkExistence(*svcFlag)
	if err != nil {
		log.Fatal("Cannot check the existence of config file.")
	}
	if !exist {
		log.Fatalf("Config file %s does not exist.", svcFlag)
	}

	config, err := loadConfig(*svcFlag)

	options := make(service.KeyValue)
	options["Restart"] = "on-success"
	options["SuccessExitStatus"] = "1 2 8 SIGKILL"
	svcConfig := &service.Config{
		Name:        "FileDispatcher",
		DisplayName: "File Dispatcher that copies files.",
		Description: "To replace the dispatcher service in common02 server for QMM.",
		Dependencies: []string{
			"Requires=network.target",
			"After=network-online.target syslog.target"},
		Option: options,
	}

	prg := &program{conf: config}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	errs := make(chan error, 5)
	logger, err = s.Logger(errs)
	if err != nil {
		log.Fatal(err)
	}

	err = s.Run()
	if err != nil {
		logger.Error(err)
	}
}
