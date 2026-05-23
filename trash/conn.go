package main

import (
	"net"
)

type Conn struct {
	conn net.Conn
}

func newConn(conn net.Conn) *Conn {
	return &Conn{
		conn: conn,
	}
}

func (c *Conn) Read() (string, error) {
	buff := make([]byte, 1000)
	n, err := c.conn.Read(buff)
	if err != nil {
		return "", err
	}

	return string(buff[:n]), nil
}

func (c *Conn) Write(msg string) error {
	_, err := c.conn.Write([]byte(msg))
	return err
}

func (c *Conn) Addr() string {
	return c.conn.LocalAddr().String()
}
