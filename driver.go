package gojdbc

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"time"
)

func init() {
	sql.Register("jdbc", &Driver{})
}

type Driver struct {
}

const (
	PARAM_TIMEOUT          = "timeout"
	CONNECTION_TEST_STRING = "d67c184ff3c42e7b7a0bf2d4bca50340"
)

func (j Driver) Open(name string) (driver.Conn, error) {
	u, e := url.Parse(name)
	if e != nil {
		return nil, e
	}

	var newConnection net.Conn
	if timeoutRaw, ok := u.Query()[PARAM_TIMEOUT]; ok && len(timeoutRaw) > 0 {
		timeout, e := strconv.ParseInt(timeoutRaw[0], 10, 64)
		if e == nil {
			newConnection, e = net.DialTimeout(u.Scheme, u.Host, time.Duration(timeout))
		}
	} else {
		newConnection, e = net.Dial(u.Scheme, u.Host)
	}

	if e != nil {
		if newConnection != nil {
			newConnection.Close()
		}
		return nil, e
	}

	dc := &DriverConnection{newConnection}
	if s, e := dc.ReadString(); e != nil {
		newConnection.Close()
		return nil, e
	} else if s != CONNECTION_TEST_STRING {
		newConnection.Close()
	}

	return &conn{dc}, nil
}

type conn struct {
	ch *DriverConnection
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	err := c.ch.WriteByte(2)
	if err != nil {
		return nil, err
	}
	id, _ := NewV4()
	err = c.ch.WriteString(id.String())
	if err != nil {
		return nil, err
	}
	err = c.ch.WriteString(query)
	if err != nil {
		return nil, err
	}
	return &stmt{conn: c, id: id.String(), query: query}, nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func (c *conn) Close() error {
	err := c.ch.WriteByte(1)
	if err != nil {
		return err
	}
	_, err = c.ch.ReadByte()
	check(err)
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return &tx{c.ch}, c.ch.WriteByte(11)
}

type tx struct {
	ch *DriverConnection
}

func (t *tx) Commit() error {
	return t.ch.WriteByte(12)
}

func (t *tx) Rollback() error {
	return t.ch.WriteByte(13)
}

func (t *tx) Stmt(stmt *sql.Stmt) *sql.Stmt {
	return stmt
}

type stmt struct {
	*conn
	id    string
	query string
}

func (s *stmt) Close() error {
	s.conn.ch.WriteByte(9)
	s.conn.ch.WriteString(s.id)
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.stage1(args)
	s.conn.ch.WriteByte(5)
	s.conn.ch.WriteString(s.id)
	b, err := s.conn.ch.ReadByte()
	check(err)
	id2, _ := NewV4()
	switch b {
	case 0:
		c, err := s.conn.ch.ReadInt32()
		check(err)
		if c < 0 {
			c = 0
		}
		return result{s.conn, id2.String(), int64(c)}, nil
	case 1:
		panic("didn't expect resultset")
	case 2:
		e, err := s.conn.ch.ReadString()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(e)
	default:
		panic("oops")
	}
}

func (s *stmt) stage1(args []driver.Value) {
	for i, x := range args {
		switch x := x.(type) {
		case int64:
			s.conn.ch.WriteByte(3)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteInt64(x)
		case string:
			s.conn.ch.WriteByte(4)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteString(x)
		case float64:
			s.conn.ch.WriteByte(8)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteFloat64(x)
		case time.Time:
			s.conn.ch.WriteByte(14)
			s.conn.ch.WriteString(s.id)
			s.conn.ch.WriteInt32(int32(i + 1))
			s.conn.ch.WriteInt64(x.UnixNano() / 1000000)
		default:
			fmt.Printf("unhandled: %T %v\n", x, x)
		}
	}
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.stage1(args)
	s.conn.ch.WriteByte(5)
	s.conn.ch.WriteString(s.id)
	b, err := s.conn.ch.ReadByte()
	check(err)
	switch b {
	case 0:
		_, err := s.conn.ch.ReadInt32()
		check(err)
		return &norows{}, nil
	case 1:
		var names, classes []string
		id2, _ := NewV4()
		s.conn.ch.WriteString(id2.String())
		n, err := s.conn.ch.ReadInt32()
		check(err)
		for i := 0; i < int(n); i++ {
			name, err := s.conn.ch.ReadString()
			check(err)
			class, err := s.conn.ch.ReadString()
			check(err)
			names = append(names, name)
			classes = append(classes, class)
		}
		return &rows{s.conn, id2.String(), names, classes}, nil
	case 2:
		e, err := s.conn.ch.ReadString()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(e)
	default:
		panic("oops")
	}
}

type rows struct {
	*conn
	id      string
	names   []string
	classes []string
}

func (r *rows) Columns() []string {
	return r.names
}
func (r *rows) Close() error {
	r.conn.ch.WriteByte(10)
	r.conn.ch.WriteString(r.id)
	return nil
}
func (r *rows) Next(dest []driver.Value) error {
	r.conn.ch.WriteByte(6)
	r.conn.ch.WriteString(r.id)
	b, err := r.conn.ch.ReadByte()
	check(err)
	if b == 0 {
		return io.EOF
	}
	for i := range r.names {
		r.conn.ch.WriteByte(7)
		r.conn.ch.WriteString(r.id)
		r.conn.ch.WriteInt32(int32(i + 1))

		dest[i] = nil

		switch r.classes[i] {
		case "java.lang.Integer":
			r.conn.ch.WriteByte(1)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt32()
				check(err)
				dest[i] = v
			}
		case "java.math.BigDecimal":
			r.conn.ch.WriteByte(10)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadString()
				check(err)
				r := big.NewRat(0, 1)
				_, ok := r.SetString(v)
				if !ok {
					panic("oops: " + v)
				}
				f, _ := r.Float64()
				dest[i] = f
			}
		case "java.lang.Long":
			r.conn.ch.WriteByte(6)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt64()
				check(err)
				dest[i] = v
			}
		case "java.lang.Short":
			r.conn.ch.WriteByte(7)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt16()
				check(err)
				dest[i] = v
			}
		case "java.lang.Byte":
			r.conn.ch.WriteByte(8)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadByte()
				check(err)
				dest[i] = v
			}
		case "java.lang.Boolean":
			r.conn.ch.WriteByte(9)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadByte()
				check(err)
				dest[i] = (v == 1)
			}
		case "java.sql.Date":
			r.conn.ch.WriteByte(5)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt64()
				check(err)
				t := time.Unix(0, v*1000000)
				t = t.In(time.UTC)
				dest[i] = t
			}
		case "java.sql.Timestamp":
			r.conn.ch.WriteByte(11)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadInt64()
				check(err)
				t := time.Unix(0, v*1000000)
				t = t.In(time.UTC)
				dest[i] = t
			}
		case "java.lang.String":
			r.conn.ch.WriteByte(2)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadString()
				check(err)
				dest[i] = []byte(v)
			}
		case "java.lang.Double":
			r.conn.ch.WriteByte(3)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadFloat64()
				check(err)
				dest[i] = v
			}
		case "java.lang.Float":
			r.conn.ch.WriteByte(4)
			b, err := r.conn.ch.ReadByte()
			check(err)
			if b == 1 {
				v, err := r.conn.ch.ReadFloat32()
				check(err)
				dest[i] = v
			}
		default:
			panic(r.classes[i])
		}
	}
	return nil
}

type result struct {
	*conn
	id2 string
	c   int64
}

func (r result) LastInsertId() (int64, error) {
	return 0, nil
}
func (r result) RowsAffected() (int64, error) {
	return r.c, nil
}

type norows struct {
	c int
}

func (r *norows) Columns() []string {
	return nil
}
func (r *norows) Close() error {
	return nil
}
func (r *norows) Next(dest []driver.Value) error {
	return fmt.Errorf("no rows")
}
