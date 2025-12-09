package main

import (
	"bytes"
)

func fn(toProcess []byte) []byte {
	return bytes.ToUpper(toProcess)
}
