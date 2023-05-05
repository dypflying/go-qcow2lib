package qcow2

import (
	"fmt"
	"unsafe"
)

func Print_Pointer_Byte(p unsafe.Pointer, bytes int, name string) {

	arr := make([]byte, bytes)
	for i := 0; i < bytes; i++ {
		arr[i] = *(*byte)(unsafe.Pointer(uintptr(p) + uintptr(i)))
	}

	fmt.Printf("name:%s,bytes = %v\n", name, arr)
}
