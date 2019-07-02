package utils

import (
	"hash/fnv"
)

//StringHash resturns the hash code of the string
func StringHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
