package qcow2

import (
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_raw_simple(t *testing.T) {
	var err error
	var filename = "/tmp/raw.txt"
	os.Remove(filename)
	var create_opts = map[string]any{
		OPT_SIZE:     1048576,
		OPT_FILENAME: filename,
		OPT_FMT:      "raw",
	}

	err = raw_create(filename, create_opts)
	assert.Nil(t, err)

	var open_opts = map[string]any{
		OPT_FILENAME: filename,
		OPT_FMT:      "raw",
	}

	bs, err := raw_open(filename, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	s := bs.opaque.(*BDRVRawState)
	assert.NotNil(t, s.File)
	raw_close(bs)
}

func Test_raw_write_read(t *testing.T) {

	var err error
	var filename = "/tmp/raw.qcow2"
	os.Remove(filename)
	var create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   filename,
		OPT_FMT:        "raw",
		OPT_SUBCLUSTER: true,
	}
	var open_opts = map[string]any{
		OPT_FILENAME: filename,
		OPT_FMT:      "raw",
	}
	err = raw_create(filename, create_opts)
	assert.Nil(t, err)

	bs, err := raw_open(filename, open_opts, BDRV_O_RDWR)
	bs.Drv = newRawDriver()
	assert.Nil(t, err)
	assert.NotNil(t, bs.Drv)

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	var qiov QEMUIOVector
	qemu_iovec_init_buf(&qiov, unsafe.Pointer(&buf[0]), bytes)
	err = raw_pwritev_part(bs, 123, bytes, &qiov, 0, 0)
	assert.Nil(t, err)

	bufOut := make([]byte, bytes)
	var qiovOut QEMUIOVector
	qemu_iovec_init_buf(&qiovOut, unsafe.Pointer(&bufOut[0]), bytes)
	err = raw_preadv_part(bs, 123, bytes, &qiovOut, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))

	raw_close(bs)
	os.Remove(filename)
}
