package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// Rename renames (moves) a file.
func (c *Client) Rename(oldpath, newpath string) error {
	_, err := c.getFileInfo(newpath)
	err = interpretException(err)
	if err != nil && !os.IsNotExist(err) {
		return &os.PathError{"rename", newpath, err}
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(true),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.Execute("rename2", req, resp)
	if err != nil {
		err = interpretException(err)
		if errors.Is(err, os.ErrExist) {
			return &os.PathError{"rename", newpath, err}
		} else {
			return &os.PathError{"rename", oldpath, err}
		}
	}

	return nil
}

func (c *Client) RenameForTrash(oldpath, newpath string) error {
	_, err := c.getFileInfo(newpath)
	err = interpretException(err)
	if err != nil && !os.IsNotExist(err) {
		return &os.PathError{"rename", newpath, err}
	}

	req := &hdfs.Rename2RequestProto{
		Src:           proto.String(oldpath),
		Dst:           proto.String(newpath),
		OverwriteDest: proto.Bool(false),
		MoveToTrash:   proto.Bool(true),
	}
	resp := &hdfs.Rename2ResponseProto{}

	err = c.namenode.Execute("rename2", req, resp)
	if err != nil {
		err = interpretException(err)
		if errors.Is(err, os.ErrExist) {
			return &os.PathError{"rename", newpath, err}
		} else {
			return &os.PathError{"rename", oldpath, err}
		}
	}
	return nil
}
