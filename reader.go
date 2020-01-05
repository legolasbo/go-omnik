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
	ReadInterval time.Duration
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

// ReadContinuous reads a sample at a given interval.
func (r *Reader) ReadContinuous(sChan chan Sample, eChan chan error) {
	ticker := time.NewTicker(r.ReadInterval)
	defer ticker.Stop()

	for {
		s, e := r.Read()
		if e != nil {
			eChan <- e
		}
		if e == nil {
			sChan <- s
		}
		<-ticker.C
	}
}
