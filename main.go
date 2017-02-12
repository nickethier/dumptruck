package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"log/syslog"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/colinmarc/hdfs"
	"github.com/docker/docker/client"
)

var pid = flag.Int("pid", 0, "process ID")
var hdfsNamenode = flag.String("hdfs", "", "hdfs namenode")
var fallbackPath = flag.String("fallback-path", "/tmp/", "fallback system path of remote fails")
var prefix = flag.String("prefix", "/coredumps/", "coredump filename prefix")
var noFallback = flag.Bool("no-fallback", false, "don't fallback to writing to path")

var cgroup = regexp.MustCompile(`.*docker\/(.*)`)
var logger = setupLogging()

func setupLogging() *log.Logger {
	l, err := syslog.NewLogger(syslog.LOG_WARNING|syslog.LOG_USER, log.Ldate|log.Ltime|log.Lshortfile|log.LUTC)
	if err != nil {
		panic(err)
	}
	return l
}

func main() {
	flag.Parse()
	logger.Println("dumptruck: recieving coredump from pid=", *pid)
	cgroups, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/cgroup", *pid))
	if err != nil {
		panic(err)
	}

	match := cgroup.FindStringSubmatch(string(cgroups))
	var storagePath string
	if len(match) == 0 {
		storagePath = buildStandardPath()
	} else {
		storagePath = buildDockerPath(match[1])
	}

	hdfsClient, err := hdfs.NewForUser(*hdfsNamenode, "root")
	if err != nil {
		logger.Println("dumptruck: error creating hdfs client, falling back to system: ", err)
		writeToSystemPath(storagePath)
		return
	}

	logger.Printf("dumptruck: writing to hdfs: path=%s", storagePath)
	hdfsClient.MkdirAll(path.Dir(storagePath), os.FileMode(0755))
	writer, err := hdfsClient.Create(storagePath)
	if err != nil {
		logger.Println("dumptruck: error creating file in hdfs, falling back to system: ", err)
		writeToSystemPath(storagePath)
		return
	}
	defer writer.Close()

	io.Copy(writer, os.Stdin)
}

func writeToSystemPath(storagePath string) {
	coreFile := path.Join(*fallbackPath, storagePath)
	os.MkdirAll(path.Dir(coreFile), os.FileMode(0700))
	writer, err := os.Create(coreFile)
	if err == nil {
		defer writer.Close()
		io.Copy(writer, os.Stdin)
	}
}

func buildStandardPath() string {
	mainPathFmt := path.Join(*prefix, "%s-%s-%s.core")
	timeStmp := time.Now().UTC().Format("20060102150405")
	hostname, _ := os.Hostname()
	cmdline, err := ioutil.ReadFile(fmt.Sprintf("/proc/%d/cmdline", *pid))
	if err != nil {
		return fmt.Sprintf(mainPathFmt, timeStmp, hostname, "pid-"+fmt.Sprintf("%d", *pid))
	}
	cmd := strings.Split(string(cmdline), string([]byte{0}))[0]
	return fmt.Sprintf(mainPathFmt, timeStmp, hostname, path.Base(cmd))
}

func buildDockerPath(id string) string {

	docker, err := client.NewEnvClient()
	if err != nil {
		panic(err)
	}

	container, err := docker.ContainerInspect(context.TODO(), id)
	if err != nil {
		panic(err)
	}

	// Format is timstamp-hostIdentifier-processIdentifier
	// For kubernetes this will include the pod name and container name
	// as the host and process identifiers respectivly
	mainPathFmt := path.Join(*prefix, "%s-%s-%s.core")
	timeStmp := time.Now().UTC().Format("20060102150405")

	if podName, ok := container.Config.Labels["io.kubernetes.pod.name"]; ok {
		containerName := container.Config.Labels["io.kubernetes.container.name"]
		return fmt.Sprintf(mainPathFmt, timeStmp, podName, containerName)
	}

	hostname, _ := os.Hostname()
	return fmt.Sprintf(mainPathFmt, timeStmp, hostname, container.Name)
}
