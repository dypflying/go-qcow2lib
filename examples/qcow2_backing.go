package main

import (
	"fmt"

	"github.com/dypflying/go-qcow2lib/qcow2"
)

const BASE_FILE = "/tmp/base.qcow2"
const OVERLAY_FILE = "/tmp/overlay.qcow2"
const BASE_OFFSET = 123

//create a base qcow2 file
func base_create() {
	var err error
	//create options
	opts := make(map[string]any)
	opts[qcow2.OPT_SIZE] = 1 << 30    //qcow2 file's size is 1g
	opts[qcow2.OPT_FMT] = "qcow2"     //qcow2 format
	opts[qcow2.OPT_SUBCLUSTER] = true //enable sub-cluster

	if err = qcow2.Blk_Create(BASE_FILE, opts); err != nil {
		fmt.Printf("failed to create qcow2 file: %s, err: %v\n", BASE_FILE, err)
	}
}

//open the base qcow2 file
func base_open() *qcow2.BdrvChild {
	var root *qcow2.BdrvChild
	var err error
	if root, err = qcow2.Blk_Open(BASE_FILE,
		map[string]any{qcow2.OPT_FMT: "qcow2"}, qcow2.BDRV_O_RDWR); err != nil {
		fmt.Printf("open failed, err: %v\n", err)
		return nil
	}
	return root
}

//write data to the qcow2 file example
func base_write(root *qcow2.BdrvChild) {
	var err error
	data := "this is a test"
	if _, err = qcow2.Blk_Pwrite(root, BASE_OFFSET, ([]byte)(data), uint64(len(data)), 0); err != nil {
		fmt.Printf("write failed, err: %v\n", err)
	}
}

//create a qcow2 file with backing example
func overlay_create() {
	var err error
	//create options
	opts := make(map[string]any)
	opts[qcow2.OPT_SIZE] = 1 << 30    //qcow2 file's size is 1g
	opts[qcow2.OPT_FMT] = "qcow2"     //qcow2 format
	opts[qcow2.OPT_SUBCLUSTER] = true //enable sub-cluster
	opts[qcow2.OPT_BACKING] = BASE_FILE

	if err = qcow2.Blk_Create(OVERLAY_FILE, opts); err != nil {
		fmt.Printf("failed to create overlay qcow2 file: %s, err: %v\n", OVERLAY_FILE, err)
	}
}

//open a qcow2 file with backing
func overlay_open() *qcow2.BdrvChild {

	var root *qcow2.BdrvChild
	var err error
	if root, err = qcow2.Blk_Open(OVERLAY_FILE,
		map[string]any{qcow2.OPT_FMT: "qcow2"}, qcow2.BDRV_O_RDWR); err != nil {
		fmt.Printf("open overlay failed, err: %v\n", err)
		return nil
	}
	return root
}

//read data from the qcow2 file example
func overlay_read(root *qcow2.BdrvChild) {
	var err error
	buf := make([]byte, 128)
	if _, err = qcow2.Blk_Pread(root, BASE_OFFSET, buf, 128); err != nil {
		fmt.Printf("read overlay failed, err: %v\n", err)
	}
	fmt.Printf("buf=%s\n", string(buf))
}

//close a qcow2 file
func close(root *qcow2.BdrvChild) {
	qcow2.Blk_Close(root)
}

/*
func main() {
	//create a base file a write something
	base_create()
	root := base_open()
	base_write(root)
	close(root)

	//create a overlay file from the base and read data
	overlay_create()
	root = overlay_open()
	overlay_read(root)
	close(root)
}
*/
