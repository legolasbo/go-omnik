package omnikreader

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

// Read reads a sample from the inverter.
func Read(ip string, serial int) Sample {
	command := GetAuthString(serial)

	conn, _ := net.Dial("tcp", ip+":8899")
	defer conn.Close()

	// send to socket
	fmt.Fprintf(conn, command)
	// listen for reply
	message, _ := bufio.NewReader(conn).ReadString('\n')

	msg := InverterMsg{
		Data: []byte(message),
	}

	return msg.GetSample(time.Now())
}
