package main

import (
	"fmt"
	"os"

	"github.com/dypflying/go-qcow2lib/qcow2"
)

const EXAMPLE_File = "/tmp/sample.qcow2"
const EXAMPLE_OFFSET = 123

//create a qcow2 file example
func Test_create() {
	var err error
	//create options
	opts := make(map[string]any)
	opts[qcow2.OPT_SIZE] = 1048576    //qcow2 file's size is 1g
	opts[qcow2.OPT_FMT] = "qcow2"     //qcow2 format
	opts[qcow2.OPT_SUBCLUSTER] = true //enable sub-cluster

	if err = qcow2.Blk_Create(EXAMPLE_File, opts); err != nil {
		fmt.Printf("failed to create qcow2 file: %s, err: %v\n", EXAMPLE_File, err)
	}
}

//open a qcow2 file example
func Test_open() *qcow2.BdrvChild {

	var root *qcow2.BdrvChild
	var err error
	if root, err = qcow2.Blk_Open(EXAMPLE_File,
		map[string]any{qcow2.OPT_FMT: "qcow2"}, os.O_RDWR|os.O_CREATE); err != nil {
		fmt.Printf("open failed, err: %v\n", err)
		return nil
	}
	return root
}

//write data to the qcow2 file example
func Test_write(root *qcow2.BdrvChild) {
	var err error
	data := "this is a test"
	if _, err = qcow2.Blk_Pwrite(root, EXAMPLE_OFFSET, ([]byte)(data), uint64(len(data)), 0); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
}

//read data from the qcow2 file example
func Test_read(root *qcow2.BdrvChild) {
	var err error
	buf := make([]byte, 128)
	if _, err = qcow2.Blk_Pread(root, EXAMPLE_OFFSET, buf, 128); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
	fmt.Printf("buf=%s\n", string(buf))
}

//close a qcow2 file
func Test_close(root *qcow2.BdrvChild) {
	qcow2.Blk_Close(root)
}

func main() {
	Test_create()
	root := Test_open()
	Test_write(root)
	Test_read(root)
	Test_close(root)
}
