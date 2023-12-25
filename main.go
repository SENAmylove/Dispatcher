/* This is a service for replacing the dispatcher service on common02 server of QMM3*/

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

func recursiveAdd(path string, watcher *fsnotify.Watcher) error {
	if err := filepath.Walk(path, func(path string, fi os.FileInfo, err error) error {
		if fi.Mode().IsDir() {
			return watcher.Add(path)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func checkIsDir(path string) (bool, error) {
	fInfo, err := os.Stat(path)
	if err != nil {
		return false, err
	}

	return fInfo.IsDir(), nil
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

	for i := 0; i < len(cf.Threads); i++ {
		cf.Threads[i].Source = filepath.ToSlash(cf.Threads[i].Source)
		cf.Threads[i].Destination = filepath.ToSlash(cf.Threads[i].Destination)

		isDir, err := checkIsDir(cf.Threads[i].Source)
		if err != nil {
			return nil, err
		}
		if !isDir {
			return nil, errors.New("Specified path:" + cf.Threads[i].Source + " is not a dir.")
		}

		isDir, err = checkIsDir(cf.Threads[i].Destination)
		if err != nil {
			return nil, err
		}
		if !isDir {
			return nil, errors.New("Specified path:" + cf.Threads[i].Destination + " is not a dir.")
		}
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
		if match, _ := filepath.Match(th.Source+"/*", path); match {
			return idx
		}
	}

	return -1
}

var logger service.Logger
var modeFlags = []string{"run", "install", "uninstall", ""}

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
		err := recursiveAdd(th.Source, watcher)
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

				event.Name = filepath.ToSlash(event.Name)

				isDir, err := checkIsDir(event.Name)
				if err != nil {
					logger.Errorf("Error on checking whether it is a dir of %s", event.Name)
					continue
				}
				if isDir {
					logger.Infof("New folder created: %s, and it would be added to watcher.", event.Name)
					err := watcher.Add(event.Name)
					if err != nil {
						logger.Errorf("Failed to add newly created folder %s into watcher.", event.Name)
						continue
					}
				} else {
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

	svcFlag := flag.String("c", "", "Specify the config json."+
		" It will use ./Dispatcher.json if not specified.")
	modeFlag := flag.String("m", "", "Specify the mode: install, uninstall, run.")
	flag.Parse()

	if *svcFlag == "" {
		execPath, err := os.Executable()
		if err != nil {
			log.Fatal("Cannot find the default config file.")
			return
		}
		*svcFlag = filepath.Join(filepath.Dir(execPath), "Dispatcher.json")
	}

	notFound := true
	for _, fl := range modeFlags {
		if *modeFlag == fl {
			notFound = false
		}
	}

	if notFound {
		log.Fatal("Unrecognized mode flag.")
		return
	}

	exist, err := checkExistence(*svcFlag)
	if err != nil {
		log.Fatal("Cannot check the existence of config file.")
	}
	if !exist {
		log.Fatalf("Config file %s does not exist.", *svcFlag)
	}

	config, err := loadConfig(*svcFlag)
	if err != nil {
		log.Fatalf("Loading config file error : %s", err)
	}

	svcConfig := &service.Config{
		Name:        "FileDispatcher",
		DisplayName: "File Dispatcher that copies files.",
		Description: "To replace the dispatcher service in common02 server for QMM.",
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

	if *modeFlag == "install" {
		err = s.Install()
		if err != nil {
			log.Fatal(err)
		}
	}

	if *modeFlag == "uninstall" {
		err = s.Uninstall()
		if err != nil {
			log.Fatal(err)
		}
	}

	if *modeFlag == "" || *modeFlag == "run" {
		err = s.Run()
		if err != nil {
			log.Fatal(err)
		}
	}
}
