package qcow2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Clz32(t *testing.T) {
	ret := clz32(1 << 0)
	assert.Equal(t, 31, ret)
	ret = clz32(1 << 1)
	assert.Equal(t, 30, ret)
	ret = clz32(1 << 3)
	assert.Equal(t, 28, ret)
	ret = clz32(1 << 5)
	assert.Equal(t, 26, ret)
	ret = clz32(1 << 7)
	assert.Equal(t, 24, ret)
	ret = clz32(1 << 9)
	assert.Equal(t, 22, ret)

	ret = clz32(1 << 11)
	assert.Equal(t, 20, ret)
	ret = clz32(1 << 13)
	assert.Equal(t, 18, ret)
	ret = clz32(1 << 15)
	assert.Equal(t, 16, ret)
	ret = clz32(1 << 17)
	assert.Equal(t, 14, ret)
	ret = clz32(1 << 19)
	assert.Equal(t, 12, ret)
	ret = clz32(1<<19 + 1<<18 + 1<<3 + 1)
	assert.Equal(t, 12, ret)
	ret = clz32(1 << 31)
	assert.Equal(t, 0, ret)
	ret = clz32(0)
	assert.Equal(t, 32, ret)
}

func Test_Clo32(t *testing.T) {
	ret := clo32(1 << 0)
	assert.Equal(t, 0, ret)
	ret = clo32(1 << 31)
	assert.Equal(t, 1, ret)
	ret = clo32(uint32(uint64(1<<32) - 1))
	assert.Equal(t, 32, ret)
	ret = clo32(0xff000000)
	assert.Equal(t, 8, ret)
	ret = clo32(0xfe000000)
	assert.Equal(t, 7, ret)
}

func Test_Ctz32(t *testing.T) {
	ret := ctz32(1 << 0)
	assert.Equal(t, 0, ret)
	ret = ctz32(1 << 1)
	assert.Equal(t, 1, ret)
	ret = ctz32(1 << 8)
	assert.Equal(t, 8, ret)
	ret = ctz32(0)
	assert.Equal(t, 32, ret)
	ret = ctz32(1<<31 + 1<<15)
	assert.Equal(t, 15, ret)
	ret = ctz32(1<<31 + 1<<15 + 1<<7)
	assert.Equal(t, 7, ret)
}

func Test_Cto32(t *testing.T) {
	ret := cto32(1 << 0)
	assert.Equal(t, 1, ret)
	ret = cto32(0)
	assert.Equal(t, 0, ret)
	ret = cto32(0x000000ff)
	assert.Equal(t, 8, ret)
	ret = cto32(7)
	assert.Equal(t, 3, ret)
	ret = cto32(uint32(uint64(1<<32) - 1))
	assert.Equal(t, 32, ret)
}
