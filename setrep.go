package hdfs

import (
	"errors"
	"os"

	hdfs "github.com/colinmarc/hdfs/v2/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

// Set replication of the file to newRep.
func (c *Client) SetReplication(name string, newRep uint32) (bool, error) {
	req := &hdfs.SetReplicationRequestProto{
		Src:         proto.String(name),
		Replication: proto.Uint32(newRep),
	}
	resp := &hdfs.SetReplicationResponseProto{}

	err := c.namenode.Execute("setReplication", req, resp)
	if err != nil {
		return false, &os.PathError{"setReplication", name, interpretException(err)}
	} else if resp.Result == nil {
		return false, &os.PathError{"setReplication", name, errors.New("unexpected empty response")}
	}

	return resp.GetResult(), nil
}
