package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/colinmarc/hdfs/v2"
)

var (
	errMultipleNamenodeUrls = errors.New("Multiple namenode URLs specified")
)

func userDir(client *hdfs.Client) string {
	return path.Join("/user", client.User())
}
func trashRootDir(client *hdfs.Client) string {
	return path.Join("/user", client.User(), ".Trash")
}

// normalizePaths parses the hosts out of HDFS (hdfs://, viewfs://, /abs/path, relative/path) URLs,
// and turns relative paths into absolute ones (by appending /user/<user>).
// viewfs://viewfsid is mapped into hdfs:// nsid. If multiple HDFS urls with
// differing hosts are passed in, it returns an error.
func normalizePaths(paths []string) ([]string, string, error) {
	conf, err := getConf()
	if err != nil {
		return nil, "", fmt.Errorf("Problem loading configuration: %s", err)
	}

	namenode := ""
	cleanPaths := make([]string, 0, len(paths))

	for _, rawurl := range paths {
		url, err := url.Parse(rawurl)
		if err != nil {
			return nil, "", err
		}

		cleanPath := url.Path
		switch url.Scheme {
		case "":
			if (!path.IsAbs(cleanPath)) {
				user, err := getUser()
				if err != nil {
					return nil, "", fmt.Errorf("Couldn't determine user: %s", err)
				}
				cleanPath = path.Join("/user", *user, cleanPath)
			}
		case "viewfs", "hdfs":
		default: return nil, "", errors.New("invalid HDFS Scheme")
		}

		if url.Host != "" {
			nsid := url.Host
			if url.Scheme == "viewfs" {
				// convert viwefs:// scheme to hdfs://
				nsid, cleanPath, err = conf.ViewfsReparseFilename("viewfs://" + nsid, cleanPath)
				if err != nil {
					return nil, "", fmt.Errorf("Reparse viewfs fail: %s", err)
				} else if strings.HasPrefix(nsid, "viewfs://") {
					return nil, "", errors.New("no mount point for " + rawurl)
				}
			}

			if namenode != "" && namenode != nsid {
				return nil, "", errMultipleNamenodeUrls
			}

			namenode = nsid
		}

		cleanPaths = append(cleanPaths, cleanPath)
	}

	// already resolved to hdfs:// nsid
	if namenode != "" || os.Getenv("HADOOP_NAMENODE") != "" {
		return cleanPaths, namenode, nil
	}

	defaultNsid := conf.DefaultNSID()
	// default fs is hdfs://
	if !strings.HasPrefix(defaultNsid, "viewfs://") {
		return cleanPaths, namenode, nil
	}

	// reparse for default viewfs
	result := make([]string, 0, len(paths))
	for _, p := range cleanPaths {
		nsid, newpath, err := conf.ViewfsReparseFilename(defaultNsid, p)
		if err != nil {
			return nil, "", fmt.Errorf("Reparse viewfs fail: %s", err)
		} else if strings.HasPrefix(nsid, "viewfs://") {
			return nil, "", errors.New("no mount point for " + defaultNsid + p)
		}

		if namenode != "" && namenode != nsid {
			return nil, "", errMultipleNamenodeUrls
		}

		namenode = nsid
		result = append(result, newpath)
	}

	if namenode == "" {
		return nil, "", errors.New("no namenode")
	}

	return result, namenode, nil
}

func getClientAndExpandedPaths(paths []string) ([]string, *hdfs.Client, error) {
	paths, nn, err := normalizePaths(paths)
	if err != nil {
		return nil, nil, err
	}

	client, err := getClient(nn)
	if err != nil {
		return nil, nil, err
	}

	expanded, err := expandPaths(client, paths)
	if err != nil {
		return nil, nil, err
	}

	return expanded, client, nil
}

// TODO: not really sure checking for a leading \ is the way to test for
// escapedness.
func hasGlob(fragment string) bool {
	match, _ := regexp.MatchString(`([^\\]|^)[[*?]`, fragment)
	return match
}

// expandGlobs recursively expands globs in a filepath. It assumes the paths
// are already cleaned and normalized (ie, absolute).
func expandGlobs(client *hdfs.Client, globbedPath string) ([]string, error) {
	parts := strings.Split(globbedPath, "/")[1:]
	var res []string
	var splitAt int

	for splitAt = range parts {
		if hasGlob(parts[splitAt]) {
			break
		}
	}

	var base, glob, next, remainder string
	base = "/" + path.Join(parts[:splitAt]...)
	glob = parts[splitAt]

	if len(parts) > splitAt+1 {
		next = parts[splitAt+1]
		remainder = path.Join(parts[splitAt+2:]...)
	} else {
		next = ""
		remainder = ""
	}

	list, err := client.ReadDir(base)
	if err != nil {
		return nil, err
	}

	for _, fi := range list {
		match, _ := path.Match(glob, fi.Name())
		if !match {
			continue
		}

		newPath := path.Join(base, fi.Name(), next, remainder)
		if hasGlob(newPath) {
			if fi.IsDir() {
				children, err := expandGlobs(client, newPath)
				if err != nil {
					return nil, err
				}

				res = append(res, children...)
			}
		} else {
			_, err := client.Stat(newPath)
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				return nil, err
			}

			res = append(res, newPath)
		}
	}

	return res, nil
}

func expandPaths(client *hdfs.Client, paths []string) ([]string, error) {
	var res []string

	for _, p := range paths {
		if hasGlob(p) {
			expanded, err := expandGlobs(client, p)
			if err != nil {
				return nil, err
			} else if len(expanded) == 0 {
				// Fake a PathError for consistency.
				return nil, &os.PathError{"stat", p, os.ErrNotExist}
			}

			res = append(res, expanded...)
		} else {
			res = append(res, p)
		}
	}

	return res, nil
}
