package utils

import (
	"hash/fnv"
	"math"
)

// IsPowerOf2 returns true if x is a power of 2, else false.
func IsPowerOf2(x int) bool {
	return x&(x-1) == 0
}

// NextPowerOf2 returns the next power of 2 >= x.
func NextPowerOf2(x int) int {
	if IsPowerOf2(x) {
		return x
	}
	return int(math.Pow(2, math.Ceil(math.Log2(float64(x)))))
}

//StringHash resturns the hash code of the string
func StringHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
