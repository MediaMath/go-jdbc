package gojdbc

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"
)

type DriverConnection struct {
	conn net.Conn
}

func (j *DriverConnection) WriteByte(i byte) error {
	if _, err := j.conn.Write([]byte{i}); err != nil {
		return err
	}
	return nil
}

func (j *DriverConnection) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	n, err := j.conn.Read(buf)
	if err != nil || n != 1 {
		return 0, err
	}
	return buf[0], nil
}

func (j *DriverConnection) WriteInt64(i int64) error {
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}
func (j *DriverConnection) WriteTime(t time.Time) error {
	i := t.UnixNano() / 1000000
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}

func (j *DriverConnection) WriteInt32(i int32) error {
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}

func (j *DriverConnection) ReadInt32() (int32, error) {
	var i int32
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *DriverConnection) ReadInt64() (int64, error) {
	var i int64
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *DriverConnection) ReadInt16() (int16, error) {
	var i int16
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *DriverConnection) WriteFloat64(i float64) error {
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		return err
	}
	return nil
}

func (j *DriverConnection) ReadFloat32() (float32, error) {
	var i float32
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *DriverConnection) ReadFloat64() (float64, error) {
	var i float64
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		return 0, err
	}
	return i, nil
}

func (j *DriverConnection) WriteString(i string) error {
	var x int32
	x = int32(len(i))
	if err := binary.Write(j.conn, binary.BigEndian, x); err != nil {
		return err
	}
	if _, err := j.conn.Write([]byte(i)); err != nil {
		return err
	}
	return nil
}

func (j *DriverConnection) ReadString() (string, error) {
	n, err := j.ReadInt32()
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	_, err = io.CopyN(buf, j.conn, int64(n))
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (j *DriverConnection) WriteBool(i bool) error {
	var x byte
	if i {
		x = 1
	} else {
		x = 0
	}
	if err := binary.Write(j.conn, binary.BigEndian, x); err != nil {
		return err
	}
	return nil
}
