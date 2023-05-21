package qcow2

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_block_simple(t *testing.T) {
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

	err = Blk_Create(filename, create_opts)
	assert.Nil(t, err)

	root, err := Blk_Open(filename, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	bs := root.GetBS()
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
	Blk_Close(root)

}

func Test_block_read_write(t *testing.T) {
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

	err = Blk_Create(filename, create_opts)
	assert.Nil(t, err)

	root, err := Blk_Open(filename, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	_, err = Blk_Pwrite(root, 123, buf, bytes, 0)
	assert.Nil(t, err)

	bufOut := make([]byte, bytes)

	_, err = Blk_Pread(root, 123, bufOut, bytes)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))

	Blk_Close(root)
	os.Remove(filename)

}

func Test_block_backing(t *testing.T) {
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
	err = Blk_Create(basefile, create_opts)
	assert.Nil(t, err)
	var open_opts = map[string]any{
		OPT_FILENAME: basefile,
		OPT_FMT:      "qcow2",
	}
	root, err := Blk_Open(basefile, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	//write to the base
	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	_, err = Blk_Pwrite(root, 123, buf, bytes, 0)
	assert.Nil(t, err)
	//close base
	Blk_Close(root)

	//create overlay on base file
	create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   overlayfile,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
		OPT_BACKING:    basefile,
	}
	err = Blk_Create(overlayfile, create_opts)
	assert.Nil(t, err)

	//open overlay
	open_opts = map[string]any{
		OPT_FILENAME: overlayfile,
		OPT_FMT:      "qcow2",
	}
	root, err = Blk_Open(overlayfile, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)
	//read the overlay
	bufOut := make([]byte, bytes)
	_, err = Blk_Pread(root, 123, bufOut, bytes)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))
	//close overlay
	Blk_Close(root)
	os.Remove(basefile)
	os.Remove(overlayfile)

}

func Test_block_backing2(t *testing.T) {
	var err error
	var basefile = "/tmp/base.qcow2"
	var overlayfile = "/tmp/overlay.qcow2"
	var overlayfile2 = "/tmp/overlay2.qcow2"
	var offset1, offset2 uint64
	offset1 = 123
	offset2 = 456

	os.Remove(basefile)
	os.Remove(overlayfile)
	os.Remove(overlayfile2)
	var create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   basefile,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
	}
	err = Blk_Create(basefile, create_opts)
	assert.Nil(t, err)
	var open_opts = map[string]any{
		OPT_FILENAME: basefile,
		OPT_FMT:      "qcow2",
	}

	root, err := Blk_Open(basefile, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	//write to the base
	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	_, err = Blk_Pwrite(root, offset1, buf, bytes, 0)
	assert.Nil(t, err)
	//close base
	Blk_Close(root)

	//create overlay on base file
	create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   overlayfile,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
		OPT_BACKING:    basefile,
	}
	err = Blk_Create(overlayfile, create_opts)
	assert.Nil(t, err)

	//open overlay
	open_opts = map[string]any{
		OPT_FILENAME: overlayfile,
		OPT_FMT:      "qcow2",
	}
	root, err = Blk_Open(overlayfile, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)
	//read the overlay

	buf = ([]byte)("this is the first overlay test")
	bytes = uint64(len(buf))
	_, err = Blk_Pwrite(root, offset1, buf, bytes, 0)
	assert.Nil(t, err)

	buf = ([]byte)("this is the first overlay test2")
	bytes = uint64(len(buf))
	_, err = Blk_Pwrite(root, offset2, buf, bytes, 0)
	assert.Nil(t, err)
	//close overlay
	Blk_Close(root)

	//create overlay on base file
	create_opts = map[string]any{
		OPT_SIZE:       1048576,
		OPT_FILENAME:   overlayfile2,
		OPT_FMT:        "qcow2",
		OPT_SUBCLUSTER: true,
		OPT_BACKING:    overlayfile,
	}
	err = Blk_Create(overlayfile2, create_opts)
	assert.Nil(t, err)

	//open overlay
	open_opts = map[string]any{
		OPT_FILENAME: overlayfile2,
		OPT_FMT:      "qcow2",
	}
	root, err = Blk_Open(overlayfile2, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	buf = ([]byte)("this is the second overlay test")
	bytes = uint64(len(buf))
	_, err = Blk_Pwrite(root, offset1, buf, bytes, 0)
	assert.Nil(t, err)

	//read the overlay
	bufOut := make([]byte, bytes)
	_, err = Blk_Pread(root, offset1, bufOut, bytes)
	assert.Nil(t, err)
	assert.Equal(t, "this is the second overlay test", string(bufOut))

	expectedStr := "this is the first overlay test2"
	bytes = uint64(len(expectedStr))
	bufOut = make([]byte, bytes)
	_, err = Blk_Pread(root, offset2, bufOut[:], bytes)
	assert.Nil(t, err)
	assert.Equal(t, "this is the first overlay test2", string(bufOut))
	Blk_Close(root)

	os.Remove(basefile)
	os.Remove(overlayfile)
	os.Remove(overlayfile2)

}

func Test_block_zeros(t *testing.T) {

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

	err = Blk_Create(filename, create_opts)
	assert.Nil(t, err)

	root, err := Blk_Open(filename, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	_, err = Blk_Pwrite_Zeroes(root, 123, bytes, 0)
	assert.Nil(t, err)

	bufOut := make([]byte, bytes)

	_, err = Blk_Pread(root, 123, bufOut[:], bytes)
	assert.Nil(t, err)
	for i := 0; i < int(bytes); i++ {
		assert.Equal(t, byte(0), bufOut[i])
	}

	Blk_Close(root)
	os.Remove(filename)
}

func Test_Discard(t *testing.T) {
	var err error
	var filename = "/tmp/test_discard.qcow2"

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

	err = Blk_Create(filename, create_opts)
	assert.Nil(t, err)

	root, err := Blk_Open(filename, open_opts, BDRV_O_RDWR|BDRV_O_UNMAP)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	_, err = Blk_Pwrite(root, 65536, buf, bytes, 0)
	assert.Nil(t, err)

	err = Blk_Discard(root, 65536, bytes)
	assert.Nil(t, err)

	bufOut := make([]byte, bytes)

	_, err = Blk_Pread(root, 65536, bufOut, bytes)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))
	var stat BlockStatistic
	scanRefcountTable(root.bs, &stat)
	assert.Equal(t, uint64(6), stat.TotalBlocks)
	assert.Equal(t, uint64(1), stat.DataBlocks)

	err = Blk_Discard(root, 65536, DEFAULT_CLUSTER_SIZE)
	assert.Nil(t, err)
	_, err = Blk_Pread(root, 65536, bufOut, bytes)
	assert.Nil(t, err)
	for i := 0; i < int(bytes); i++ {
		assert.Equal(t, byte(0), bufOut[i])
	}

	var stat2 BlockStatistic
	scanRefcountTable(root.bs, &stat2)
	assert.Equal(t, uint64(5), stat2.TotalBlocks)
	assert.Equal(t, uint64(0), stat2.DataBlocks)

	Blk_Close(root)
	os.Remove(filename)

}

func Test_data_file(t *testing.T) {
	var err error
	var filename = "/tmp/test_datafile.qcow2"
	var datafile = "/tmp/datafile.img"

	os.Remove(filename)
	os.Remove(datafile)
	var create_opts = map[string]any{
		OPT_SIZE:     1048576,
		OPT_FILENAME: filename,
		OPT_FMT:      "qcow2",
		//OPT_SUBCLUSTER: false,
		OPT_SUBCLUSTER: true,
		OPT_DATAFILE:   datafile,
	}

	var open_opts = map[string]any{
		OPT_FILENAME: filename,
		OPT_FMT:      "qcow2",
	}

	err = Blk_Create(filename, create_opts)
	assert.Nil(t, err)

	root, err := Blk_Open(filename, open_opts, BDRV_O_RDWR)
	assert.Nil(t, err)
	assert.NotNil(t, root)

	buf := ([]byte)("this is a test")
	bytes := uint64(len(buf))
	_, err = Blk_Pwrite(root, 123, buf, bytes, 0)
	assert.Nil(t, err)

	bufOut := make([]byte, bytes)

	_, err = Blk_Pread(root, 123, bufOut, bytes)
	assert.Nil(t, err)
	assert.Equal(t, "this is a test", string(bufOut))

	Blk_Close(root)
	os.Remove(filename)
	os.Remove(datafile)
}
