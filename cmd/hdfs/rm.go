package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs/v2"
)

func rm(paths []string, recursive bool, force bool, skipTrash bool, forceTrash bool) {
	expanded, client, err := getClientAndExpandedPaths(paths)
	if err != nil {
		fatal(err)
	}

	for _, p := range expanded {
		info, err := client.Stat(p)
		if err != nil {
			if force && os.IsNotExist(err) {
				continue
			}

			if pathErr, ok := err.(*os.PathError); ok {
				pathErr.Op = "remove"
			}

			fmt.Fprintln(os.Stderr, err)
			status = 1
			continue
		}

		if !recursive && info.IsDir() {
			fmt.Fprintln(os.Stderr, &os.PathError{"remove", p, errors.New("file is a directory")})
			status = 1
			continue
		}

		if !skipTrash {
			// always enable trash on client side
			success, err := moveToTrash(client, p, forceTrash)
			if force && errors.Is(err, os.ErrNotExist) {
				continue
			} else if err != nil {
				fatal(err)
			} else if success {
				continue
			}
		}

		// skipTrash or moveToTrash() returns false without error
		err = client.RemoveAll(p)
		if err != nil {
			fatal(err)
		}
	}
}

const PERMISSION = os.FileMode(0o700)

// implement like org.apache.hadoop.fs.Trash.moveToTrash(Path path).
func moveToTrash(c *hdfs.Client, name string, forceTrash bool) (bool, error) {
	var err error

	if !forceTrash {
		// check trash is enabled on the server side
		defaults, err := c.ServerDefaults()
		if err != nil {
			return false, err
		}
		if defaults.TrashInterval <= 0 {
			// trash is disabled on the server side
			// fallback to delete
			return false, nil
		}
	}

	trashRoot := trashRootDir(c)
	if strings.HasPrefix(name, trashRoot) {
		return false, nil // already in trash, fallback to delete
	}

	if strings.HasPrefix(path.Dir(trashRoot), name) {
		return false, errors.New("Cannot move \"" + name + "\" to the trash, as it contains the trash")
	}

	trashPath := path.Join(trashRoot, "Current", name);
	baseTrashPath := path.Dir(trashPath);

	// try twice, in case checkpoint between the mkdirs() & rename()
	for i := 0; i < 2; i++ {
		err = c.MkdirAll(baseTrashPath, PERMISSION)
		if err != nil {
			if errors.Is(err, os.ErrExist) || errors.Is(err, syscall.ENOTDIR) {
				// find the path which is not a directory, and modify baseTrashPath
				// & trashPath, then mkdirs
				existsFilePath := baseTrashPath;
				exists := false
				for !exists {
					exists, err = c.Exists(existsFilePath)
					if err != nil {
						return false, err
					} else if !exists {
						existsFilePath = path.Dir(existsFilePath);
					}
				}
				baseTrashPath = strings.Replace(baseTrashPath, existsFilePath, existsFilePath + strconv.FormatInt(time.Now().UnixMilli(), 10), 1)
				trashPath = path.Join(baseTrashPath, path.Base(trashPath))

				// retry, ignore current failure
				i--
				continue
			} else {
				fmt.Fprintln(os.Stderr, "Can't create trash directory: " + baseTrashPath)
				return false, err
			}
		}

		// if the target path in Trash already exists, then append with
		// a current time in millisecs.
		orig := trashPath
		exists := true

		for exists {
			exists, err = c.Exists(trashPath)
			if err != nil {
				break
			} else if exists {
				trashPath = orig + strconv.FormatInt(time.Now().UnixMilli(), 10)
			}
		}
		if err != nil {
			continue
		}

		// move to current trash
		err = c.RenameForTrash(name, trashPath)
		if err == nil {
			fmt.Fprintln(os.Stdout, "Moved: 'hdfs://" + c.NSID() + name + "' to trash at: hdfs://" + c.NSID() + trashPath)
			return true, nil
		}
	}

	if err == nil {
		err = &os.PathError{"remove", name, errors.New("Failed to move " + name + " to trash " + trashPath)}
	}
	return false, err
}
