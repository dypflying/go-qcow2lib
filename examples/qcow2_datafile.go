package main

import (
	"fmt"
	"os"

	"github.com/dypflying/go-qcow2lib/qcow2"
)

func main() {
	var err error
	var filename = "/tmp/test_datafile.qcow2"
	var datafile = "/tmp/datafile.img"
	var root *qcow2.BdrvChild

	os.Remove(filename)
	os.Remove(datafile)
	var create_opts = map[string]any{
		qcow2.OPT_SIZE:       1048576,
		qcow2.OPT_FILENAME:   filename,
		qcow2.OPT_FMT:        "qcow2",
		qcow2.OPT_SUBCLUSTER: false,
		qcow2.OPT_DATAFILE:   datafile,
	}

	var open_opts = map[string]any{
		qcow2.OPT_FILENAME: filename,
		qcow2.OPT_FMT:      "qcow2",
	}

	if err = qcow2.Blk_Create(filename, create_opts); err != nil {
		panic(err)
	}

	if root, err = qcow2.Blk_Open(filename, open_opts, qcow2.BDRV_O_RDWR); err != nil {
		panic(err)
	}

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	if _, err = qcow2.Blk_Pwrite(root, 123, buf, bytes, 0); err != nil {
		panic(err)
	}

	bufOut := make([]byte, bytes)

	if _, err = qcow2.Blk_Pread(root, 123, bufOut, bytes); err != nil {
		panic(err)
	}
	fmt.Printf("read buf = %s\n", string(bufOut))

	qcow2.Blk_Close(root)
	//os.Remove(filename)
	//os.Remove(datafile)

}
