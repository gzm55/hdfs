package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/colinmarc/hdfs/v2"
)

func setrep(args []string) {
	if len(args) < 2 {
		fatalWithUsage()
	}

	newRep, err := strconv.ParseUint(args[0], 10, 32)
	if err != nil {
		fatal(err)
	} else if newRep < 1 {
		fatal(errors.New("replication must be >= 1"))
	}

	expanded, client, err := getClientAndExpandedPaths(args[1:])
	if err != nil {
		fatal(err)
	}

	// non-exist path, dir, ref-link or file with ec are ignored
	for _, p := range expanded {
		// now p is a directory
		err = client.Walk(p, func(path string, info os.FileInfo, err error) error {
			if (err != nil && errors.Is(err, os.ErrNotExist)) || info == nil {
				// ignore non-exist path
				return nil
			} else if err != nil {
				return err
			} else if !info.(*hdfs.FileInfo).IsFile() {
				// ignore dir or symlink path
				return nil
			}

			// file with ec are ignored
			success, err := client.SetReplication(path, uint32(newRep))
			if success {
				fmt.Fprintln(os.Stdout, "Replication " + args[0] + " set: hdfs://" + client.NSID() + path)
			}

			return err
		})
		if err != nil {
			fatal(err)
		}
	}
}


