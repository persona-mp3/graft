package graft

import (
	"net"
)

type peer struct {
	conn net.Conn
}

func (p peer) Read() ([]byte, error) {
	buffer := make([]byte, 1024)
	n, err := p.conn.Read(buffer)
	return buffer[:n], err
}

func (p peer) Write(content string) error {
	_, err := p.conn.Write([]byte(content))
	if err != nil {
		return err
	}
	return nil
}
