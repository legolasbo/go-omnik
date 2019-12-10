package omnikreader

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

// Read reads a sample from the inverter.
func Read(ip string, serial int) (Sample, error) {
	command := GetAuthString(serial)

	conn, err := net.Dial("tcp", ip+":8899")
	if err != nil {
		fmt.Println("Couldn't connect to inverter:", err)
		return Sample{}, err
	}
	defer conn.Close()

	// send to socket
	fmt.Fprintf(conn, command)
	// listen for reply
	message, _ := bufio.NewReader(conn).ReadString('\n')

	msg := InverterMsg{
		Data: []byte(message),
	}

	return msg.GetSample(time.Now()), nil
}
