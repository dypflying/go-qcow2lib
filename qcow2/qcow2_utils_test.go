package qcow2

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_Start_Of_Cluster(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
	}
	res := start_of_cluster(s, 1)
	if res != 0 {
		t.Errorf("result: %d, expected: %d\n", res, 0)
	}
	res = start_of_cluster(s, uint64(s.ClusterSize)-1)
	if res != 0 {
		t.Errorf("result: %d, expected: %d\n", res, 0)
	}
	res = start_of_cluster(s, uint64(s.ClusterSize))
	if res != uint64(s.ClusterSize) {
		t.Errorf("result: %d, expected: %d\n", res, int64(s.ClusterSize))
	}
	res = start_of_cluster(s, uint64(s.ClusterSize)+1)
	if res != uint64(s.ClusterSize) {
		t.Errorf("result: %d, expected: %d\n", res, int64(s.ClusterSize))
	}
}

func Test_Offset_Into_Cluster(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
	}
	res := offset_into_cluster(s, 1)
	if res != 1 {
		t.Errorf("result: %d, expected: %d\n", res, 1)
	}
	res = offset_into_cluster(s, uint64(s.ClusterSize)-1)
	if res != 65535 {
		t.Errorf("result: %d, expected: %d\n", res, int64(s.ClusterSize)-1)
	}
	res = offset_into_cluster(s, uint64(s.ClusterSize))
	if res != 0 {
		t.Errorf("result: %d, expected: %d\n", res, 0)
	}
	//t.Logf("start1 = %d\n", start1)
}

func Test_Size_To_Clusters(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
	}
	n1 := size_to_clusters(s, 0)
	n2 := size_to_clusters(s, 1)
	n3 := size_to_clusters(s, DEFAULT_CLUSTER_SIZE)
	n4 := size_to_clusters(s, DEFAULT_CLUSTER_SIZE+1)
	assert.Equal(t, uint64(0), n1)
	assert.Equal(t, uint64(1), n2)
	assert.Equal(t, uint64(1), n3)
	assert.Equal(t, uint64(2), n4)

}

func Test_Size_To_L1(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
	}
	n1 := size_to_l1(s, 0)
	n2 := size_to_l1(s, DEFAULT_CLUSTER_SIZE*1024*8)   //512M
	n3 := size_to_l1(s, DEFAULT_CLUSTER_SIZE*1024*8+1) //512M+1
	assert.Equal(t, uint64(0), n1)
	assert.Equal(t, uint64(1), n2)
	assert.Equal(t, uint64(2), n3)
}

func Test_Offset_To_L1_Index(t *testing.T) {

	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
	}
	n1 := offset_to_l1_index(s, 0)
	n2 := offset_to_l1_index(s, DEFAULT_CLUSTER_SIZE*1024*8-1) //512M
	n3 := offset_to_l1_index(s, DEFAULT_CLUSTER_SIZE*1024*8)   //512M+1
	assert.Equal(t, uint64(0), n1)
	assert.Equal(t, uint64(0), n2)
	assert.Equal(t, uint64(1), n3)
}

func Test_Offset_To_L2_Index(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
		L2Size:      1 << (DEFAULT_CLUSTER_BITS - 3),
	}
	n1 := offset_to_l2_index(s, 0)
	n2 := offset_to_l2_index(s, DEFAULT_CLUSTER_SIZE*1024*8-1) //512M
	n3 := offset_to_l2_index(s, DEFAULT_CLUSTER_SIZE*1024*8)   //512M+1
	assert.Equal(t, uint64(0), n1)
	assert.Equal(t, uint64(8191), n2)
	assert.Equal(t, uint64(0), n3)
}

func Test_Offset_To_L2_Slice_Index(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
		L2Size:      1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize: 1 << (DEFAULT_CLUSTER_BITS - 3),
	}
	n1 := offset_to_l2_slice_index(s, 0)
	n2 := offset_to_l2_slice_index(s, DEFAULT_CLUSTER_SIZE*1024*8-1) //512M
	n3 := offset_to_l2_slice_index(s, DEFAULT_CLUSTER_SIZE*1024*8)   //512M+1
	assert.Equal(t, uint64(0), n1)
	assert.Equal(t, uint64(8191), n2)
	assert.Equal(t, uint64(0), n3)
}

func Test_Offset_To_Sc_Index(t *testing.T) {

}

func Test_Get_L2_Entry(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
		L2Size:      1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize: 1 << (DEFAULT_CLUSTER_BITS - 3),
	}
	n := 8192
	table := make([]uint64, n)
	table[0] = cpu_to_be64(123)
	table[n/2] = cpu_to_be64(456)
	table[n-1] = cpu_to_be64(789)
	val1 := get_l2_entry(s, unsafe.Pointer(&table[0]), 0)
	val2 := get_l2_entry(s, unsafe.Pointer(&table[0]), uint32(n/2))
	val3 := get_l2_entry(s, unsafe.Pointer(&table[0]), uint32(n-1))
	assert.Equal(t, uint64(123), val1)
	assert.Equal(t, uint64(456), val2)
	assert.Equal(t, uint64(789), val3)
}

func Test_Set_L2_Entry(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
		L2Size:      1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize: 1 << (DEFAULT_CLUSTER_BITS - 3),
	}
	n := 8192
	table := make([]uint64, n)
	set_l2_entry(s, unsafe.Pointer(&table[0]), 0, uint64(123))
	set_l2_entry(s, unsafe.Pointer(&table[0]), uint32(n/2), uint64(456))
	set_l2_entry(s, unsafe.Pointer(&table[0]), uint32(n-1), uint64(789))
	assert.Equal(t, uint64(123), be64_to_cpu(table[0]))
	assert.Equal(t, uint64(456), be64_to_cpu(table[n/2]))
	assert.Equal(t, uint64(789), be64_to_cpu(table[n-1]))
}

func Test_Set_L2_Bitmap(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize:          DEFAULT_CLUSTER_SIZE,
		ClusterBits:          DEFAULT_CLUSTER_BITS,
		L2Bits:               DEFAULT_CLUSTER_BITS - 3,
		L2Size:               1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize:          1 << (DEFAULT_CLUSTER_BITS - 3),
		IncompatibleFeatures: QCOW2_INCOMPAT_EXTL2,
	}
	n := 8192
	table := make([]uint64, n*2)
	set_l2_bitmap(s, unsafe.Pointer(&table[0]), 0, uint64(123))
	set_l2_bitmap(s, unsafe.Pointer(&table[0]), uint32(n/2), uint64(456))
	set_l2_bitmap(s, unsafe.Pointer(&table[0]), uint32(n-1), uint64(789))
	assert.Equal(t, uint64(123), be64_to_cpu(table[1]))
	assert.Equal(t, uint64(456), be64_to_cpu(table[n+1]))
	assert.Equal(t, uint64(789), be64_to_cpu(table[2*n-1]))
}

func Test_L2_Entry_Size(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize:          DEFAULT_CLUSTER_SIZE,
		ClusterBits:          DEFAULT_CLUSTER_BITS,
		L2Bits:               DEFAULT_CLUSTER_BITS - 3,
		L2Size:               1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize:          1 << (DEFAULT_CLUSTER_BITS - 3),
		IncompatibleFeatures: QCOW2_INCOMPAT_EXTL2,
	}
	assert.Equal(t, uint64(16), l2_entry_size(s))

	s = &BDRVQcow2State{
		ClusterSize: DEFAULT_CLUSTER_SIZE,
		ClusterBits: DEFAULT_CLUSTER_BITS,
		L2Bits:      DEFAULT_CLUSTER_BITS - 3,
		L2Size:      1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize: 1 << (DEFAULT_CLUSTER_BITS - 3),
	}

	assert.Equal(t, uint64(8), l2_entry_size(s))
}

func Test_Get_L2_Bitmap(t *testing.T) {
	s := &BDRVQcow2State{
		ClusterSize:          DEFAULT_CLUSTER_SIZE,
		ClusterBits:          DEFAULT_CLUSTER_BITS,
		L2Bits:               DEFAULT_CLUSTER_BITS - 3,
		L2Size:               1 << (DEFAULT_CLUSTER_BITS - 3),
		L2SliceSize:          1 << (DEFAULT_CLUSTER_BITS - 3),
		IncompatibleFeatures: QCOW2_INCOMPAT_EXTL2,
	}
	n := 8192
	table := make([]uint64, n*2)
	table[1] = cpu_to_be64(123)
	table[n+1] = cpu_to_be64(456)
	table[2*n-1] = cpu_to_be64(789)
	assert.Equal(t, uint64(123), get_l2_bitmap(s, unsafe.Pointer(&table[0]), 0))
	assert.Equal(t, uint64(456), get_l2_bitmap(s, unsafe.Pointer(&table[0]), uint32(n/2)))
	assert.Equal(t, uint64(789), get_l2_bitmap(s, unsafe.Pointer(&table[0]), uint32(n-1)))
}
