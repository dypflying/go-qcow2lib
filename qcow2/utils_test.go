package qcow2

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_Cpu_To_Be64(t *testing.T) {
	var val uint64 = 3
	valBe := cpu_to_be64(val)
	valCpu := be64_to_cpu(valBe)
	assert.Equal(t, uint64(val<<56), valBe)
	assert.Equal(t, uint64(3), valCpu)

	val = 255
	valBe = cpu_to_be64(val)
	valCpu = be64_to_cpu(valBe)
	assert.Equal(t, uint64(val<<56), valBe)
	assert.Equal(t, uint64(255), valCpu)

	val = 256
	valBe = cpu_to_be64(val)
	valCpu = be64_to_cpu(valBe)
	assert.Equal(t, uint64(1<<48), valBe)
	assert.Equal(t, uint64(256), valCpu)
}

func Test_Memset(t *testing.T) {

	n := 1024
	table := make([]int, n)
	table[0] = 123
	table[n/2] = 456
	table[n-1] = 789
	memset(unsafe.Pointer(&table[0]), n*SIZE_INT)
	for i := 0; i < n; i++ {
		if table[i] != 0 {
			t.Errorf("memset not all zero")
		}
	}
}

func Test_Memcpy(t *testing.T) {
	n := 1024
	table := make([]int, n)
	table[0] = 123
	table[n/2] = 456
	table[n-1] = 789
	dst_table := make([]int, n)

	memcpy(unsafe.Pointer(&dst_table[0]), unsafe.Pointer(&table[0]), uint64(n)*uint64(SIZE_INT))

	assert.Equal(t, 123, dst_table[0])
	assert.Equal(t, 456, dst_table[n/2])
	assert.Equal(t, 789, dst_table[n-1])
}

func Test_Align_Up(t *testing.T) {
	n1 := align_up(1, 512)
	n2 := align_up(512, 512)
	n3 := align_up(513, 512)
	assert.Equal(t, 512, n1)
	assert.Equal(t, 512, n2)
	assert.Equal(t, 1024, n3)
}
func Test_Align_Down(t *testing.T) {
	n1 := align_down(1, 512)
	n2 := align_down(512, 512)
	n3 := align_down(513, 512)
	assert.Equal(t, 0, n1)
	assert.Equal(t, 512, n2)
	assert.Equal(t, 512, n3)
}

func Test_Round_Up(t *testing.T) {

	ret := round_up(1, 65536)
	assert.Equal(t, 65536, ret)
	ret = round_up(0, 65536)
	assert.Equal(t, 0, ret)
	ret = round_up(65536, 65536)
	assert.Equal(t, 65536, ret)
	ret = round_up(65537, 65536)
	assert.Equal(t, 131072, ret)
}

func Test_Round_Down(t *testing.T) {

	ret := round_down(1, 65536)
	assert.Equal(t, 0, ret)
	ret = round_down(0, 65536)
	assert.Equal(t, 0, ret)
	ret = round_down(65536, 65536)
	assert.Equal(t, 65536, ret)
	ret = round_down(65537, 65536)
	assert.Equal(t, 65536, ret)
}

func Test_Max(t *testing.T) {

	arr := [5]uint64{56778, 12345, 23432445, 112363, 5434523}
	max := max(arr[0], arr[1], arr[2], arr[3], arr[4])
	assert.Equal(t, uint64(23432445), max)
}

func Test_Min(t *testing.T) {
	arr := [5]uint64{56778, 12345, 23432445, 112363, 5434523}
	min := min(arr[0], arr[1], arr[2], arr[3], arr[4])
	assert.Equal(t, uint64(12345), min)
}
