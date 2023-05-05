package qcow2

/*
//TODO1
func Test_qcow2_create(t *testing.T) {
	opt := &CreateOpts{
		Filename: "/tmp/test.qcow2",
		Size:     1073741824 * 4,
	}

	bs, err := qcow2_create(opt)
	assert.NotNil(t, bs)
	assert.Nil(t, err)
	assert.NotNil(t, bs.opaque)
	assert.Equal(t, bs.opaque.ClusterSize, uint32(DEFAULT_CLUSTER_SIZE))
	assert.Equal(t, bs.opaque.ClusterBits, uint32(DEFAULT_CLUSTER_BITS))
	assert.Equal(t, bs.opaque.RefcountTableOffset, uint64(DEFAULT_CLUSTER_SIZE))
	assert.Equal(t, bs.opaque.L1TableOffset, uint64(DEFAULT_CLUSTER_SIZE*3))
	assert.NotNil(t, bs.current.file)

	//check RefcountTable
	//assert.Equal(t, bs.opaque.RefcountTable[0], uint64(DEFAULT_CLUSTER_SIZE*2))
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
	qcow2_cache_flush(bs, bs.opaque.RefcountBlockCache)

	//check refcount block
	refcountArray := make([]byte, 20)

	_, err = Bdrv_Direct_Pread(bs.current, DEFAULT_CLUSTER_SIZE*2, refcountArray, 20)
	assert.Nil(t, err)
	for i := int(0); i < 8; i += 2 {
		refcount := *(*uint16)(unsafe.Pointer(&refcountArray[i]))
		assert.Equal(t, refcount, uint16(1))
	}
	for i := int(8); i < 20; i += 2 {
		refcount := *(*uint16)(unsafe.Pointer(&refcountArray[i]))
		assert.Equal(t, refcount, uint16(0))
	}
}

//TODO1
func Test_QCow2_Open(t *testing.T) {
	opt := &OpenOpts{}
	opt.Filename = "/tmp/test.qcow2"

	bs, err := QCow2_Open(opt)
	assert.NotNil(t, bs)
	assert.Nil(t, err)
	assert.NotNil(t, bs.opaque)
	assert.Equal(t, bs.opaque.ClusterSize, uint32(DEFAULT_CLUSTER_SIZE))
	assert.Equal(t, bs.opaque.ClusterBits, uint32(DEFAULT_CLUSTER_BITS))
	assert.Equal(t, bs.opaque.RefcountTableOffset, uint64(DEFAULT_CLUSTER_SIZE))
	assert.Equal(t, bs.opaque.L1TableOffset, uint64(DEFAULT_CLUSTER_SIZE*3))
	assert.NotNil(t, bs.current.file)

	//check refcount block
	refcountArray := make([]byte, 20)
	_, err = Bdrv_Direct_Pread(bs.current, DEFAULT_CLUSTER_SIZE*2, refcountArray, 20)
	//t.Logf("%b\n", refcountArray)
	assert.Nil(t, err)
	for i := int(0); i < 8; i += 2 {
		refcount := *(*uint16)(unsafe.Pointer(&refcountArray[i]))
		assert.Equal(t, refcount, uint16(1))
	}
	for i := int(8); i < 20; i += 2 {
		refcount := *(*uint16)(unsafe.Pointer(&refcountArray[i]))
		assert.Equal(t, refcount, uint16(0))
	}

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

}
*/
