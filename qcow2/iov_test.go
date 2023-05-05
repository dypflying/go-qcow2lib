package qcow2

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_Qemu_Iovec_Init_Buf(t *testing.T) {

	//test values
	length := uint64(1024)
	idx1 := uint64(3)
	idx2 := uint64(6)

	buf := make([]uint64, length)
	buf[idx1] = 1234
	buf[idx2] = 4321
	var qiov QEMUIOVector

	Qemu_Iovec_Init_Buf(&qiov, unsafe.Pointer(&buf[0]), 8*length)

	assert.Equal(t, qiov.local_iov.iov_len, length*8)
	assert.Equal(t, qiov.niov, 1)
	assert.NotNil(t, qiov.iov)

	val1 := *(*uint64)(unsafe.Pointer(uintptr(qiov.local_iov.iov_base) + uintptr(idx1*8)))
	val6 := *(*uint64)(unsafe.Pointer(uintptr(qiov.local_iov.iov_base) + uintptr(idx2*8)))
	assert.Equal(t, val1, buf[idx1])
	assert.Equal(t, val6, buf[idx2])

	val1 = *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx1*8)))
	val6 = *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx2*8)))
	assert.Equal(t, val1, buf[idx1])
	assert.Equal(t, val6, buf[idx2])
}

func Test_Qemu_Iovec_Init_Add(t *testing.T) {

	//test values
	length1 := uint64(5678)
	idx1 := uint64(3)
	idx2 := uint64(6)
	buf := make([]uint64, length1)
	buf[idx1] = 1234
	buf[idx2] = 4321

	qiov := New_QEMUIOVector()

	Qemu_Iovec_Init(qiov, 1)
	//test empty qiov
	assert.NotNil(t, qiov)
	assert.NotNil(t, qiov.iov)
	assert.Equal(t, qiov.niov, 0)
	assert.Equal(t, qiov.nalloc, 1)
	assert.Equal(t, len(qiov.iov), 1)

	//test add first buffer
	Qemu_Iovec_Add(qiov, unsafe.Pointer(&buf[0]), length1)
	assert.Equal(t, qiov.niov, 1)
	assert.Equal(t, qiov.nalloc, 1)
	assert.Equal(t, qiov.size, length1)

	val1 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx1*8)))
	val2 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx2*8)))
	assert.Equal(t, val1, buf[idx1])
	assert.Equal(t, val2, buf[idx2])

	//test add second buffer
	length2 := uint64(4321)
	idx2_1 := uint64(2)
	idx2_2 := uint64(1256)
	buf2 := make([]uint64, length2)
	buf2[idx2_1] = 236897
	buf2[idx2_2] = 5468495
	Qemu_Iovec_Add(qiov, unsafe.Pointer(&buf2[0]), length2)
	assert.Equal(t, qiov.niov, 2)
	assert.Equal(t, qiov.nalloc, 3)
	assert.Equal(t, qiov.size, length1+length2)

	val2_1 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[1].iov_base) + uintptr(idx2_1*8)))
	val2_2 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[1].iov_base) + uintptr(idx2_2*8)))
	assert.Equal(t, val2_1, buf2[idx2_1])
	assert.Equal(t, val2_2, buf2[idx2_2])

}

func Test_Qiov_Slice(t *testing.T) {

	qiov := New_QEMUIOVector()
	Qemu_Iovec_Init(qiov, 1)
	buf1 := make([]uint64, 1024)
	buf2 := make([]uint64, 2048)
	buf3 := make([]uint64, 1024)
	Qemu_Iovec_Add(qiov, unsafe.Pointer(&buf1[0]), 1024)
	Qemu_Iovec_Add(qiov, unsafe.Pointer(&buf2[0]), 2048)
	Qemu_Iovec_Add(qiov, unsafe.Pointer(&buf3[0]), 1024)
	head := uint64(0)
	tail := uint64(0)
	var niov int
	iov1 := Qiov_Slice(qiov, 2024, 1024, &head, &tail, &niov)
	t.Logf("niov = %d, head = %d, tail = %d\n", niov, head, tail)
	t.Logf("1: iov_len = %d\n", iov1[0].iov_len)
	t.Logf("2: iov_len = %d\n", iov1[1].iov_len)
	assert.Equal(t, uint64(1000), head)
	assert.Equal(t, uint64(24), tail)
	assert.Equal(t, 1, niov)

	Qiov_Slice(qiov, 2024, 2024, &head, &tail, &niov)
	t.Logf("niov = %d, head = %d, tail = %d\n", niov, head, tail)
	assert.Equal(t, uint64(1000), head)
	assert.Equal(t, uint64(48), tail)
	assert.Equal(t, 2, niov)
}

func Test_Qemu_Iovec_Init_Extended(t *testing.T) {

	qiov := New_QEMUIOVector()
	Qemu_Iovec_Init(qiov, 1)
	head_buf := make([]byte, 512)
	//Qemu_Iovec_Add(qiov, unsafe.Pointer(&buf1[0]), 1024)

	midQiov := New_QEMUIOVector()
	Qemu_Iovec_Init(midQiov, 1)
	mid_buf := make([]byte, 2048)
	Qemu_Iovec_Add(midQiov, unsafe.Pointer(&mid_buf[0]), 2048)

	tail_buf := make([]byte, 512)

	Qemu_Iovec_Init_Extended(qiov, unsafe.Pointer(&head_buf[0]), 512, midQiov, 512, 1024, unsafe.Pointer(&tail_buf[0]), 512)
	assert.Equal(t, 3, qiov.niov)
	assert.Equal(t, 3, qiov.nalloc)
	assert.Equal(t, 3, len(qiov.iov))
	assert.Equal(t, uint64(512), qiov.iov[0].iov_len)
	assert.Equal(t, uint64(1024), qiov.iov[1].iov_len)
	assert.Equal(t, uint64(512), qiov.iov[2].iov_len)

	t.Logf("qiov.niov=%d, nalloc=%d, len(iov)=%d\n", qiov.niov, qiov.nalloc, len(qiov.iov))
	t.Logf("qiov.iov[0] length=%d\n", qiov.iov[0].iov_len)
	t.Logf("qiov.iov[1] length=%d\n", qiov.iov[1].iov_len)
	t.Logf("qiov.iov[2] length=%d\n", qiov.iov[2].iov_len)

}

func Test_Qemu_Iovec_Concat(t *testing.T) {
	dst_qiov := New_QEMUIOVector()
	Qemu_Iovec_Init(dst_qiov, 1)
	buf1 := make([]uint64, 1024)
	buf2 := make([]uint64, 2048)
	buf3 := make([]uint64, 1024)
	Qemu_Iovec_Add(dst_qiov, unsafe.Pointer(&buf1[0]), 1024)
	Qemu_Iovec_Add(dst_qiov, unsafe.Pointer(&buf2[0]), 2048)
	Qemu_Iovec_Add(dst_qiov, unsafe.Pointer(&buf3[0]), 1024)

	src_qiov := New_QEMUIOVector()
	Qemu_Iovec_Init(src_qiov, 1)
	buf := make([]uint64, 1024)
	Qemu_Iovec_Add(src_qiov, unsafe.Pointer(&buf[0]), 1024)

	//do concat
	Qemu_Iovec_Concat(dst_qiov, src_qiov, 512, 1024)
	assert.Equal(t, 4, dst_qiov.niov)
	assert.Equal(t, uint64(512), dst_qiov.iov[3].iov_len)

	Qemu_Iovec_Concat(dst_qiov, src_qiov, 512, 24)
	assert.Equal(t, 5, dst_qiov.niov)
	assert.Equal(t, uint64(24), dst_qiov.iov[4].iov_len)

}
