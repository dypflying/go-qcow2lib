package main

import (
	"fmt"
	"os"

	"github.com/dypflying/go-qcow2lib/qcow2"
)

const ZERO_FILE = "/tmp/zero.qcow2"
const ZERO_OFFSET = 123

//create a qcow2 file example
func zero_create() {
	var err error
	//create options
	opts := make(map[string]any)
	opts[qcow2.OPT_SIZE] = 1 << 30    //qcow2 file's size is 1g
	opts[qcow2.OPT_FMT] = "qcow2"     //qcow2 format
	opts[qcow2.OPT_SUBCLUSTER] = true //enable sub-cluster

	if err = qcow2.Blk_Create(ZERO_FILE, opts); err != nil {
		fmt.Printf("failed to create qcow2 file: %s, err: %v\n", ZERO_FILE, err)
	}
}

//open a qcow2 file example
func zero_open() *qcow2.BdrvChild {

	var root *qcow2.BdrvChild
	var err error
	if root, err = qcow2.Blk_Open(ZERO_FILE,
		map[string]any{qcow2.OPT_FMT: "qcow2"}, os.O_RDWR|os.O_CREATE); err != nil {
		fmt.Printf("open failed, err: %v\n", err)
		return nil
	}
	return root
}

//write data to the qcow2 file example
func zero_write(root *qcow2.BdrvChild) {
	var err error
	if _, err = qcow2.Blk_Pwrite_Zeroes(root, ZERO_OFFSET, 128, 0); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
}

//read data from the qcow2 file example
func zero_read(root *qcow2.BdrvChild) {
	var err error
	buf := make([]byte, 128)
	if _, err = qcow2.Blk_Pread(root, ZERO_OFFSET, buf, 128); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
	fmt.Printf("buf=%s\n", string(buf))
}

//close a qcow2 file
func zero_close(root *qcow2.BdrvChild) {
	qcow2.Blk_Close(root)
}

func main() {
	zero_create()
	root := zero_open()
	zero_write(root)
	zero_read(root)
	zero_close(root)
}
