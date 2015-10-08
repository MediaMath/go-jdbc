package gojdbc

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"io"
	"log"
	"net"
	"time"
)

type driverConnection struct {
	conn net.Conn
}

func (j *driverConnection) Close() error {
	return j.conn.Close()
}

func (j *driverConnection) WriteByte(i byte) error {
	if _, err := j.conn.Write([]byte{i}); err != nil {
		log.Println("Driver connection error", err)
		return driver.ErrBadConn
	}
	return nil
}

func (j *driverConnection) ReadByte() (byte, error) {
	buf := make([]byte, 1)
	n, err := j.conn.Read(buf)
	if err != nil || n != 1 {
		log.Println("Driver connection error", err)
		return 0, driver.ErrBadConn
	}
	return buf[0], nil
}

func (j *driverConnection) WriteInt64(i int64) error {
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		log.Println("Driver connection error", err)
		return driver.ErrBadConn
	}
	return nil
}

func (j *driverConnection) WriteTime(t time.Time) error {
	i := t.UnixNano() / 1000000
	return j.WriteInt64(i)
}

func (j *driverConnection) WriteInt32(i int32) error {
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		log.Println("Driver connection error", err)
		return driver.ErrBadConn
	}
	return nil
}

func (j *driverConnection) ReadInt32() (int32, error) {
	var i int32
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		log.Println("Driver connection error", err)
		return 0, driver.ErrBadConn
	}
	return i, nil
}

func (j *driverConnection) ReadInt64() (int64, error) {
	var i int64
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		log.Println("Driver connection error", err)
		return 0, driver.ErrBadConn
	}
	return i, nil
}

func (j *driverConnection) ReadInt16() (int16, error) {
	var i int16
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		log.Println("Driver connection error", err)
		return 0, driver.ErrBadConn
	}
	return i, nil
}

func (j *driverConnection) WriteFloat64(i float64) error {
	if err := binary.Write(j.conn, binary.BigEndian, i); err != nil {
		log.Println("Driver connection error", err)
		return driver.ErrBadConn
	}
	return nil
}

func (j *driverConnection) ReadFloat32() (float32, error) {
	var i float32
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		log.Println("Driver connection error", err)
		return 0, driver.ErrBadConn
	}
	return i, nil
}

func (j *driverConnection) ReadFloat64() (float64, error) {
	var i float64
	if err := binary.Read(j.conn, binary.BigEndian, &i); err != nil {
		log.Println("Driver connection error", err)
		return 0, driver.ErrBadConn
	}
	return i, nil
}

func (j *driverConnection) WriteString(i string) (e error) {
	// Write length then the string
	if e = j.WriteInt32(int32(len(i))); e == nil {
		_, e = j.conn.Write([]byte(i))
	}
	if e != nil {
		log.Println("Driver connection error", e)
		return driver.ErrBadConn
	}
	return
}

func (j *driverConnection) ReadString() (string, error) {
	n, err := j.ReadInt32()
	if err != nil {
		log.Println("Driver connection error", err)
		return "", driver.ErrBadConn
	}
	buf := new(bytes.Buffer)
	_, err = io.CopyN(buf, j.conn, int64(n))
	if err != nil {
		log.Println("Driver connection error", err)
		return "", driver.ErrBadConn
	}
	return buf.String(), nil
}

func (j *driverConnection) WriteBool(i bool) error {
	var x byte
	if i {
		x = 1
	} else {
		x = 0
	}
	if err := binary.Write(j.conn, binary.BigEndian, x); err != nil {
		log.Println("Driver connection error", err)
		return driver.ErrBadConn
	}
	return nil
}

// Common error approach
func (j *driverConnection) CheckError() (string, error) {
	returnCode, e := j.ReadByte()
	if e != nil {
		log.Println("Driver connection error", e)
		return "", driver.ErrBadConn
	}
	switch returnCode {
	case 0:
		return "", nil
	case 1:
		errMessage, e := j.ReadString()
		if e != nil {
			log.Println("Driver connection error", e)
			return "", driver.ErrBadConn
		}
		return errMessage, nil

	}
	log.Printf("Driver connection error, unknown code: %d", int(returnCode))
	return "", driver.ErrBadConn
}
