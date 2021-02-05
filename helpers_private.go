package bokchoy_redis

import (
	"unsafe"
)

func buildKey1(p1 string) string {
	return "bokchoy/" + p1
}

func buildKey2(p1, p2 string) string {
	buf := make([]byte, 0, 9+len(p1)+len(p2))
	buf = append(buf, "bokchoy/"...)
	buf = append(buf, p1...)
	buf = append(buf, '/')
	buf = append(buf, p2...)
	return *(*string)(unsafe.Pointer(&buf))
}
