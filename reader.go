package omnik

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

// InverterInfo holds the connection information for a given inverter.
type InverterInfo struct {
	IP string
	Serial int
}

// Reader is capable of connecting to an Omnik converter and reading it's status.
type Reader struct {
	Inverter InverterInfo
}

// Read reads a sample from the inverter.
func (r *Reader) Read() (Sample, error) {
	command := GetAuthString(r.Inverter.Serial)

	conn, err := net.Dial("tcp", r.Inverter.IP+":8899")
	if err != nil {
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
