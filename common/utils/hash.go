package utils

import (
	_ "runtime"
	"unsafe"
)

type stringStruct struct {
	str unsafe.Pointer
	len int
}

// noescape：用于禁用逃逸分析，linkname：用于重命名导出的函数或变量
//
//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHash 用于计算字节数组哈希的快速哈希函数
func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

// // MemHashString is the hash function used by go map, it utilizes available hardware instructions
// // (behaves as aeshash if aes instruction is available).
// // NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
// func MemHashString(str string) uint64 {
// 	ss := (*stringStruct)(unsafe.Pointer(&str))
// 	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
// }
