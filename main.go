package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/exec"

	worker "github.com/contribsys/faktory_worker_go"
	"github.com/pelletier/go-toml/v2"
)

var jobDefMap map[string]JobDef

type JobDefs struct {
	Jobs []JobDef
}

type JobDef struct {
	Name string
	Cmd  []string
}

func loadJobsFile(jobsFile string) error {
	f, err := os.Open(jobsFile)
	if err != nil {
		return err
	}

	defs := JobDefs{}
	dec := toml.NewDecoder(f)
	if err = dec.Decode(&defs); err != nil {
		return err
	}

	log.Printf("Loaded %v: %v\n", jobsFile, defs)

	jobDefMap = make(map[string]JobDef)
	for _, def := range defs.Jobs {
		jobDefMap[def.Name] = def
	}

	return nil
}

func perform(ctx context.Context, args ...interface{}) error {
	help := worker.HelperFor(ctx)

	jobArg, err := json.Marshal(args)
	if err != nil {
		log.Printf("Working on %s#%s: error: %v\n", help.JobType(), help.Jid(), err)
		return err
	}

	def, _ := jobDefMap[help.JobType()]
	cmd := make([]string, len(def.Cmd))
	copy(cmd, def.Cmd)
	cmd = append(cmd, string(jobArg))

	log.Printf("Working on %s#%s\n", help.JobType(), help.Jid())

	c := exec.Command(cmd[0], cmd[1:]...)
	if err = c.Run(); err != nil {
		log.Printf("Working on %s#%s: error: %v\n", help.JobType(), help.Jid(), err)
		return err
	}

	log.Printf("Completed %s#%s\n", help.JobType(), help.Jid())

	return nil
}

func main() {
	jobsFile := "jobs.toml"
	if len(os.Args) > 1 {
		jobsFile = os.Args[1]
	}
	if err := loadJobsFile(jobsFile); err != nil {
		log.Fatalf("Failed to load %s: %v\n", jobsFile, err)
	}

	mgr := worker.NewManager()
	mgr.Concurrency = 4
	mgr.ProcessWeightedPriorityQueues(map[string]int{"critical": 3, "default": 2, "bulk": 1})

	for name, _ := range jobDefMap {
		mgr.Register(name, perform)
	}

	mgr.Run()
}
