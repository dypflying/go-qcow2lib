package qcow2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Qcow2_Cache_Get_Table_Addr(t *testing.T) {
	cache := qcow2_cache_create(nil, 4, 65536)
	p0 := qcow2_cache_get_table_addr(cache, 0)
	p1 := qcow2_cache_get_table_addr(cache, 1)
	p2 := qcow2_cache_get_table_addr(cache, 2)
	span1 := uint64(uintptr(p1) - uintptr(p0))
	span2 := uint64(uintptr(p2) - uintptr(p0))
	assert.Equal(t, uint64(65536), span1)
	assert.Equal(t, uint64(65536*2), span2)
}

func Test_Qcow2_Cache_Get_Table_Idx(t *testing.T) {
	cache := qcow2_cache_create(nil, 4, 65536)
	p1 := qcow2_cache_get_table_addr(cache, 1)
	p2 := qcow2_cache_get_table_addr(cache, 2)
	idx1 := qcow2_cache_get_table_idx(cache, p1)
	idx2 := qcow2_cache_get_table_idx(cache, p2)
	assert.Equal(t, 1, idx1)
	assert.Equal(t, 2, idx2)
}
