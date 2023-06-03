package main

import (
	"fmt"

	"github.com/dypflying/go-qcow2lib/qcow2"
)

const SIMPLE_FILE = "/tmp/simple.qcow2"
const SIMPLE_OFFSET = 123

//create a qcow2 file example
func simple_create() {
	var err error
	//create options
	opts := make(map[string]any)
	opts[qcow2.OPT_SIZE] = 1 << 30    //qcow2 file's size is 1g
	opts[qcow2.OPT_FMT] = "qcow2"     //qcow2 format
	opts[qcow2.OPT_SUBCLUSTER] = true //enable sub-cluster

	if err = qcow2.Blk_Create(SIMPLE_FILE, opts); err != nil {
		fmt.Printf("failed to create qcow2 file: %s, err: %v\n", SIMPLE_FILE, err)
	}
}

//open a qcow2 file example
func simple_open() *qcow2.BdrvChild {

	var root *qcow2.BdrvChild
	var err error
	if root, err = qcow2.Blk_Open(SIMPLE_FILE,
		map[string]any{qcow2.OPT_FMT: "qcow2"}, qcow2.BDRV_O_RDWR); err != nil {
		fmt.Printf("open failed, err: %v\n", err)
		return nil
	}
	return root
}

//write data to the qcow2 file example
func simple_write(root *qcow2.BdrvChild) {
	var err error
	data := "this is a test"
	if _, err = qcow2.Blk_Pwrite(root, SIMPLE_OFFSET, ([]byte)(data), uint64(len(data)), qcow2.BDRV_REQ_FUA); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
}

//write data to the qcow2 file example
func simple_write2(root *qcow2.BdrvChild) {
	var err error
	data := "this is a test"
	if _, err = qcow2.Blk_Pwrite(root, SIMPLE_OFFSET+655360, ([]byte)(data), uint64(len(data)), 0); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
}

//read data from the qcow2 file example
func simple_read(root *qcow2.BdrvChild) {
	var err error
	buf := make([]byte, 128)
	if _, err = qcow2.Blk_Pread(root, SIMPLE_OFFSET, buf, 128); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
	fmt.Printf("buf=%s\n", string(buf))
}

//close a qcow2 file
func simple_close(root *qcow2.BdrvChild) {
	qcow2.Blk_Close(root)
}

func main() {
	simple_create()
	root := simple_open()
	fmt.Println("----------------")
	simple_write(root)
	//simple_write2(root)
	fmt.Println("----------------")
	simple_read(root)
	simple_close(root)
}
