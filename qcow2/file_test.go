package qcow2

import (
	"context"
	"os"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_Pwritev_Preadv(t *testing.T) {
	//test values
	length1 := uint64(1024)
	idx1_1 := uint64(3)
	idx1_2 := uint64(6)
	buf1 := make([]uint64, length1)
	buf1[idx1_1] = 1234
	buf1[idx1_2] = 4321

	length2 := uint64(2048)
	idx2_1 := uint64(2011)
	idx2_2 := uint64(1234)
	buf2 := make([]uint64, length2)
	buf2[idx2_1] = 5678
	buf2[idx2_2] = 6789

	qiov := New_QEMUIOVector()

	qemu_iovec_init(qiov, 1)
	qemu_iovec_add(qiov, unsafe.Pointer(&buf1[0]), length1*8)
	qemu_iovec_add(qiov, unsafe.Pointer(&buf2[0]), length2*8)
	assert.Equal(t, 2, qiov.niov)
	file, err := os.OpenFile("/tmp/test.txt", os.O_CREATE|os.O_RDWR, os.FileMode(0755))
	assert.Nil(t, err)

	n, err := pwritev(context.Background(), file, qiov.iov, qiov.niov, 512)
	assert.Nil(t, err)
	assert.Equal(t, uint64(3072*8), n)

	//now memset all the buffer
	val1 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx1_1*8)))
	assert.Equal(t, uint64(1234), val1)
	memset(qiov.iov[0].iov_base, int(qiov.iov[0].iov_len))
	val1 = *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx1_1*8)))
	assert.Equal(t, uint64(0), val1)
	memset(qiov.iov[1].iov_base, int(qiov.iov[1].iov_len))

	//now read the buffer from the file
	n, err = preadv(context.Background(), file, qiov.iov, qiov.niov, 512)
	assert.Nil(t, err)
	assert.Equal(t, uint64(3072*8), n)

	val1_1 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx1_1*8)))
	val1_2 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[0].iov_base) + uintptr(idx1_2*8)))
	val2_1 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[1].iov_base) + uintptr(idx2_1*8)))
	val2_2 := *(*uint64)(unsafe.Pointer(uintptr(qiov.iov[1].iov_base) + uintptr(idx2_2*8)))
	assert.Equal(t, uint64(1234), val1_1)
	assert.Equal(t, uint64(4321), val1_2)
	assert.Equal(t, uint64(5678), val2_1)
	assert.Equal(t, uint64(6789), val2_2)
	file.Close()
}
