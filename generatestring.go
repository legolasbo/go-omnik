package omnikreader

import (
	"encoding/hex"
	"fmt"
)

// GenerateString generates an authentication string for the socket.
func GenerateString(serial int) string {
	doubleHex := fmt.Sprintf("%x%x", serial, serial)

	var stringList []byte
	for i := len(doubleHex) - 2; i >= 0; i -= 2 {
		r, _ := hex.DecodeString(doubleHex[i : i+2])
		stringList = append(stringList, r[0])
	}

	csCount := 115
	for _, v := range stringList {
		csCount += int(v)
	}
	hexCount := fmt.Sprintf("%x", csCount)

	csString, _ := hex.DecodeString(hexCount[len(hexCount)-2 : len(hexCount)])
	checksum := string(csString)

	start := "\x68\x02\x40\x30"
	hex := string(stringList)
	separator := "\x01\x00"
	end := "\x16"

	retVal := start + hex + separator + string(checksum) + end
	return retVal
}
