package qcow2

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_get_set_refcount(t *testing.T) {

	refcountArray := make([]uint16, 1<<15)
	set_refcount(unsafe.Pointer(&refcountArray[0]), 1, 3)
	val := get_refcount(unsafe.Pointer(&refcountArray[0]), 1)
	assert.Equal(t, uint16(3), val)

	set_refcount(unsafe.Pointer(&refcountArray[0]), 11, 0)
	val = get_refcount(unsafe.Pointer(&refcountArray[0]), 11)
	assert.Equal(t, uint16(0), val)

	set_refcount(unsafe.Pointer(&refcountArray[0]), 111, 65535)
	val = get_refcount(unsafe.Pointer(&refcountArray[0]), 111)
	assert.Equal(t, uint16(65535), val)

}
