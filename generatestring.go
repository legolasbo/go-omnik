package omnik

import (
	"encoding/hex"
	"fmt"
)

var (
	authStrings map[int]string
)

// GetAuthString retrieves an authentication string for the socket.
func GetAuthString(serial int) string {
	s := getCachedAuthString(serial)

	if s != "" {
		return s
	}

	s = generateString(serial)
	cacheAuthString(serial, s)
	return s
}

func getCachedAuthString(serial int) string {
	if authStrings == nil {
		authStrings = make(map[int]string)
		return ""
	}

	if s, ok := authStrings[serial]; ok {
		return s
	}
	
	return ""
}

func generateString(serial int) string {
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

func cacheAuthString(s int, a string) {
	if authStrings == nil {
		authStrings = make(map[int]string)
	}

	authStrings[s] = a
}
