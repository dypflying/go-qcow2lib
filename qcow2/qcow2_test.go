package qcow2

import (
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_qcow2_simple(t *testing.T) {
	var err error
	var filename = "/tmp/test.qcow2"
	os.Remove(filename)
	var create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   filename,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
	}

	var open_opts = map[string]any{
		OPT_FILENAME: filename,
		OPT_FMT:      "qcow2",
	}

	err = qcow2_create(filename, create_opts)
	assert.Nil(t, err)

	bs, err := qcow2_open(filename, open_opts, os.O_RDWR)
	assert.Nil(t, err)
	s := bs.opaque.(*BDRVQcow2State)
	assert.NotNil(t, bs.opaque)
	assert.Equal(t, s.ClusterSize, uint32(DEFAULT_CLUSTER_SIZE))
	assert.Equal(t, s.ClusterBits, uint32(DEFAULT_CLUSTER_BITS))
	assert.Equal(t, s.RefcountTableOffset, uint64(DEFAULT_CLUSTER_SIZE))
	assert.Equal(t, s.L1TableOffset, uint64(DEFAULT_CLUSTER_SIZE*3))
	assert.NotNil(t, s.DataFile)
	assert.Equal(t, s.SubclusterBits, uint64(11))
	assert.Equal(t, s.SubclustersPerCluster, uint64(32))
	assert.Equal(t, s.SubclusterSize, uint64(2048))

	//check RefcountTable
	assert.Equal(t, s.RefcountTable[0], uint64(DEFAULT_CLUSTER_SIZE*2))
	//check cache
	cluster1Ref, err := qcow2_get_refcount(bs, 0)
	assert.Nil(t, err)
	assert.Equal(t, cluster1Ref, uint16(1))
	cluster4Ref, err := qcow2_get_refcount(bs, 3)
	assert.Nil(t, err)
	assert.Equal(t, cluster4Ref, uint16(1))
	cluster5Ref, err := qcow2_get_refcount(bs, 4)
	assert.Nil(t, err)
	assert.Equal(t, cluster5Ref, uint16(0))

	//flush the cache
	qcow2_cache_flush(bs, s.RefcountBlockCache)

	//check refcount block
	refcountArray := make([]byte, 20)

	_, err = bdrv_direct_pread(bs.current, DEFAULT_CLUSTER_SIZE*2, refcountArray, 20)
	assert.Nil(t, err)
	for i := int(0); i < 8; i += 2 {
		refcount := *(*uint16)(unsafe.Pointer(&refcountArray[i]))
		assert.Equal(t, be16_to_cpu(refcount), uint16(1))
	}
	for i := int(8); i < 20; i += 2 {
		refcount := *(*uint16)(unsafe.Pointer(&refcountArray[i]))
		assert.Equal(t, be16_to_cpu(refcount), uint16(0))
	}
	qcow2_close(bs)
	os.Remove(filename)

}

func Test_qcow2_write_read(t *testing.T) {
	var err error
	var filename = "/tmp/test.qcow2"
	os.Remove(filename)
	var create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   filename,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
	}

	var open_opts = map[string]any{
		OPT_FILENAME: filename,
		OPT_FMT:      "qcow2",
	}

	err = qcow2_create(filename, create_opts)
	assert.Nil(t, err)

	bs, err := qcow2_open(filename, open_opts, os.O_RDWR)
	bs.Drv = newQcow2Driver()
	assert.Nil(t, err)
	assert.NotNil(t, bs.Drv)

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	var qiov QEMUIOVector
	Qemu_Iovec_Init_Buf(&qiov, unsafe.Pointer(&buf[0]), bytes)
	err = qcow2_pwritev_part(bs, 123, bytes, &qiov, 0, 0)
	assert.Nil(t, err)

	bufOut := make([]byte, bytes)
	var qiovOut QEMUIOVector
	Qemu_Iovec_Init_Buf(&qiovOut, unsafe.Pointer(&bufOut[0]), bytes)
	err = qcow2_preadv_part(bs, 123, bytes, &qiovOut, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))

	qcow2_close(bs)
	os.Remove(filename)

}

func Test_qcow2_backup_write_read(t *testing.T) {
	var err error
	var basefile = "/tmp/base.qcow2"
	var overlayfile = "/tmp/overlay.qcow2"
	os.Remove(basefile)
	os.Remove(overlayfile)
	var create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   basefile,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
	}
	err = qcow2_create(basefile, create_opts)
	assert.Nil(t, err)
	var open_opts = map[string]any{
		OPT_FILENAME: basefile,
		OPT_FMT:      "qcow2",
	}
	//open base
	bs, err := qcow2_open(basefile, open_opts, os.O_RDWR)
	bs.Drv = newQcow2Driver()
	assert.Nil(t, err)
	//write to the base
	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	var qiov QEMUIOVector
	Qemu_Iovec_Init_Buf(&qiov, unsafe.Pointer(&buf[0]), bytes)
	err = qcow2_pwritev_part(bs, 123, bytes, &qiov, 0, 0)
	assert.Nil(t, err)
	//close base
	qcow2_close(bs)

	//create overlay on base file
	create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   overlayfile,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
		OPT_BACKING:    basefile,
	}
	err = qcow2_create(overlayfile, create_opts)
	assert.Nil(t, err)

	//open overlay
	open_opts = map[string]any{
		OPT_FILENAME: overlayfile,
		OPT_FMT:      "qcow2",
	}
	bs, err = qcow2_open(overlayfile, open_opts, os.O_RDWR)
	bs.Drv = newQcow2Driver()
	assert.Nil(t, err)
	//read from the overlay
	bufOut := make([]byte, bytes)
	var qiovOut QEMUIOVector
	Qemu_Iovec_Init_Buf(&qiovOut, unsafe.Pointer(&bufOut[0]), bytes)
	err = qcow2_preadv_part(bs, 123, bytes, &qiovOut, 0, 0)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))

	qcow2_close(bs)
	os.Remove(basefile)
	os.Remove(overlayfile)

}

func Test_qcow2_write_read_zeros(t *testing.T) {
	var err error
	var filename = "/tmp/test_zero.qcow2"
	os.Remove(filename)
	var create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   filename,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
	}

	var open_opts = map[string]any{
		OPT_FILENAME: filename,
		OPT_FMT:      "qcow2",
	}

	err = qcow2_create(filename, create_opts)
	assert.Nil(t, err)

	bs, err := qcow2_open(filename, open_opts, os.O_RDWR)
	bs.Drv = newQcow2Driver()
	assert.Nil(t, err)
	assert.NotNil(t, bs.Drv)

	bytes := uint64(128)
	err = qcow2_pwrite_zeroes(bs, 123, bytes, BDRV_REQ_ZERO_WRITE)
	assert.Nil(t, err)

	//read from the overlay
	bufOut := make([]byte, bytes)
	var qiovOut QEMUIOVector
	Qemu_Iovec_Init_Buf(&qiovOut, unsafe.Pointer(&bufOut[0]), bytes)
	err = qcow2_preadv_part(bs, 123, bytes, &qiovOut, 0, 0)
	assert.Nil(t, err)
	s := *(*string)(unsafe.Pointer(&bufOut[0]))
	assert.Equal(t, "", s)

	qcow2_close(bs)
	os.Remove(filename)
}
