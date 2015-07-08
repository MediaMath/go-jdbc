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
	"sync"
	"time"
)

const (
	_            = iota
	COMMAND_DONE = iota
	COMMAND_PREPARE
	COMMAND_SETLONG
	COMMAND_SETSTRING
	COMMAND_EXECUTE
	COMMAND_NEXT
	COMMAND_GET
	COMMAND_SETDOUBLE
	COMMAND_CLOSE_STATEMENT
	COMMAND_CLOSE_RESULT_SET
	COMMAND_BEGIN_TRANSACTION
	COMMAND_COMMIT_TRANSACTION
	COMMAND_ROLLBACK_TRANSACTION
	COMMAND_SETTIME
	COMMAND_SETNULL

	COMMAND_CLOSE_CONNECTION = -1
)

//const (
// _        = iota
// TYPE_INT = iota
// TYPE_STRING
// TYPE_DOUBLE
// TYPE_FLOAT
// TYPE_TIME
// TYPE_LONG
// TYPE_SHORT
// TYPE_BYTE
// TYPE_BOOLEAN
// TYPE_BIG_DECIMAL
// TYPE_TIMESTAMP
//)

func init() {
	sql.Register("jdbc", &Driver{})
}

type Driver struct {
}

const (
	PARAM_TIMEOUT          = "timeout"
	CONNECTION_TEST_STRING = "d67c184ff3c42e7b7a0bf2d4bca50340"
	BUFFER_SIZE            = 1000
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
		if newConnection, e = net.Dial(u.Scheme, u.Host); e == nil {
			if tcp, ok := newConnection.(*net.TCPConn); ok {
				e = tcp.SetLinger(-1)
			}
		}
	}

	if e != nil {
		if newConnection != nil {
			newConnection.Close()
		}
		return nil, e
	}

	dc := &driverConnection{conn: newConnection}
	if s, e := dc.ReadString(); e != nil {
		newConnection.Close()
		return nil, e
	} else if s != CONNECTION_TEST_STRING {
		newConnection.Close()
	}

	return &conn{openStmt: map[string]*stmt{}, dc: dc}, nil
}

type conn struct {
	openStmt     map[string]*stmt
	openStmtLock sync.Mutex
	dc           *driverConnection
	tx           *tx
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	err := c.dc.WriteByte(COMMAND_PREPARE)
	if err != nil {
		return nil, err
	}
	id, _ := NewV4()
	err = c.dc.WriteString(id.String())
	if err != nil {
		return nil, err
	}
	err = c.dc.WriteString(query)
	if err != nil {
		return nil, err
	}
	if e := c.dc.CheckError(); e != nil {
		return nil, e
	}

	s := &stmt{conn: c, id: id.String(), query: query}
	c.openStmtLock.Lock()
	defer c.openStmtLock.Unlock()
	c.openStmt[s.id] = s
	return s, nil
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func (c *conn) Close() (e error) {
	c.openStmtLock.Lock()
	defer c.openStmtLock.Unlock()
	defer c.dc.Close()
	for _, s := range c.openStmt {
		go s.Close()
	}
	return
}

func (c *conn) putStmt(s *stmt) {
	c.openStmtLock.Lock()
	defer c.openStmtLock.Unlock()
	delete(c.openStmt, s.id)
}

func (c *conn) Begin() (driver.Tx, error) {
	c.tx = &tx{c: c}
	return c.tx, c.dc.WriteByte(11)

}

type tx struct {
	c        *conn
	finished bool
}

func (t *tx) finish() bool {
	if t.finished {
		return false
	}
	t.finished = true
	t.c.tx = nil
	return true
}

func (t *tx) Commit() error {
	if !t.finish() {
		return nil
	}
	if e := t.c.dc.WriteByte(12); e != nil {
		return e
	}
	return t.c.dc.CheckError()
}

func (t *tx) Rollback() error {
	if !t.finish() {
		return nil
	}
	if e := t.c.dc.WriteByte(13); e != nil {
		return e
	}
	return t.c.dc.CheckError()
}

type stmt struct {
	conn           *conn
	id             string
	query          string
	closed         bool
	stmtCloseMutex sync.Mutex
}

// TODO: Do not close if in use by any queries
func (s *stmt) Close() (e error) {
	s.stmtCloseMutex.Lock()
	defer s.stmtCloseMutex.Unlock()
	if !s.closed {
		defer s.conn.putStmt(s)
		s.closed = true
		if e = s.conn.dc.WriteByte(COMMAND_CLOSE_STATEMENT); e == nil {
			e = s.conn.dc.WriteString(s.id)
		}
	}
	return
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.stage1(args)
	s.conn.dc.WriteByte(COMMAND_EXECUTE)
	s.conn.dc.WriteString(s.id)
	b, err := s.conn.dc.ReadByte()
	check(err)
	switch b {
	case 0:
		var c int32

		if s.conn.tx == nil {
			c, err = s.conn.dc.ReadInt32()
			check(err)
			if c < 0 {
				c = 0
			}
		}

		return result{s.conn.dc, int64(c)}, nil
	case 1:
		panic("didn't expect resultset")
	case 2:
		errorMessage, err := s.conn.dc.ReadString()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errorMessage)
	default:
		panic("oops")
	}
}

func (s *stmt) stage1(args []driver.Value) {
	for i, x := range args {
		switch xT := x.(type) {
		case int64:
			s.conn.dc.WriteByte(COMMAND_SETLONG)
			s.conn.dc.WriteString(s.id)
			s.conn.dc.WriteInt32(int32(i + 1))
			s.conn.dc.WriteInt64(xT)
		case string:
			s.conn.dc.WriteByte(COMMAND_SETSTRING)
			s.conn.dc.WriteString(s.id)
			s.conn.dc.WriteInt32(int32(i + 1))
			s.conn.dc.WriteString(xT)
		case float64:
			s.conn.dc.WriteByte(COMMAND_SETDOUBLE)
			s.conn.dc.WriteString(s.id)
			s.conn.dc.WriteInt32(int32(i + 1))
			s.conn.dc.WriteFloat64(xT)
		case time.Time:
			s.conn.dc.WriteByte(COMMAND_SETTIME)
			s.conn.dc.WriteString(s.id)
			s.conn.dc.WriteInt32(int32(i + 1))
			s.conn.dc.WriteInt64(xT.UnixNano() / 1000000)
		case nil:
			s.conn.dc.WriteByte(COMMAND_SETNULL)
			s.conn.dc.WriteString(s.id)
			s.conn.dc.WriteInt32(int32(i + 1))
		default:
			fmt.Printf("unhandled(%d): %T %v %T %v\n", i, x, x, xT, xT)
		}
	}
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.stage1(args)
	s.conn.dc.WriteByte(COMMAND_EXECUTE)
	s.conn.dc.WriteString(s.id)
	b, err := s.conn.dc.ReadByte()
	check(err)
	switch b {
	// No row
	case 0:
		_, err := s.conn.dc.ReadInt32()
		check(err)
		return &norows{}, nil

	// Results
	case 1:
		var names, classes []string
		id2, _ := NewV4()
		s.conn.dc.WriteString(id2.String())
		n, err := s.conn.dc.ReadInt32()
		check(err)
		for i := 0; i < int(n); i++ {
			name, err := s.conn.dc.ReadString()
			check(err)
			class, err := s.conn.dc.ReadString()
			check(err)
			names = append(names, name)
			classes = append(classes, class)
		}
		return &rows{
			driverConnection: s.conn.dc,
			id:               id2.String(),
			names:            names,
			classes:          classes,
			hasMore:          true,
		}, nil

	// Error
	case 2:
		e, err := s.conn.dc.ReadString()
		if err != nil {
			return nil, err
		}
		return nil, errors.New(e)
	default:
		panic("oops")
	}
}

// TODO: Tell stat when closed
type rows struct {
	*driverConnection
	id         string
	names      []string
	classes    []string
	currentRow int
	buffer     [][]driver.Value
	hasMore    bool
	closed     bool
}

func (r *rows) Columns() []string {
	return r.names
}
func (r *rows) Close() (e error) {
	if !r.closed {
		r.closed = true
		if e = r.WriteByte(COMMAND_CLOSE_RESULT_SET); e == nil {
			e = r.WriteString(r.id)
		}
	}
	return
}

func (r *rows) Next(dest []driver.Value) error {
	if l := len(r.buffer); r.currentRow >= l {
		// Hit the end of this result set
		if !r.hasMore {
			return io.EOF
		} else if e := r.bufferNext(); e != nil {
			return e
		}
	}
	// Still nothing to give, end of result set
	if len(r.buffer) == 0 {
		return io.EOF
	}

	for i, v := range r.buffer[r.currentRow] {
		dest[i] = v
	}
	r.currentRow = r.currentRow + 1
	return nil
}

func (r *rows) bufferNext() error {
	r.buffer = make([][]driver.Value, 0, BUFFER_SIZE)
	r.currentRow = 0
	nameLength := len(r.names)

	r.WriteByte(COMMAND_NEXT)
	r.WriteInt32(int32(BUFFER_SIZE))
	r.WriteString(r.id)

	for i := 0; i < BUFFER_SIZE; i++ {
		if b, err := r.ReadByte(); err != nil {
			return err
		} else if b == 0 {
			r.hasMore = false
			break
		}

		dest := make([]driver.Value, nameLength)
		for i := range r.names {

			dest[i] = nil
			if b, err := r.ReadByte(); err != nil {
				return err
			} else if b != 1 {
				continue
			}

			switch r.classes[i] {
			case "java.lang.Integer":
				v, err := r.ReadInt32()
				if err != nil {
					return err
				}
				dest[i] = v

			case "java.math.BigDecimal":
				v, err := r.ReadString()
				if err != nil {
					return err
				}
				rational := big.NewRat(0, 1)
				_, ok := rational.SetString(v)
				if !ok {
					panic("oops: " + v)
				}
				f, _ := rational.Float64()
				dest[i] = f

			case "java.lang.Long":
				v, err := r.ReadInt64()
				if err != nil {
					return err
				}
				dest[i] = v

			case "java.lang.Short":
				v, err := r.ReadInt16()
				if err != nil {
					return err
				}
				dest[i] = v

			case "java.lang.Byte":
				v, err := r.ReadByte()
				if err != nil {
					return err
				}
				dest[i] = v

			case "java.lang.Boolean":
				v, err := r.ReadByte()
				if err != nil {
					return err
				}
				dest[i] = (v == 1)

			case "java.sql.Date":
				v, err := r.ReadInt64()
				if err != nil {
					return err
				}
				t := time.Unix(0, v*1000000)
				t = t.In(time.UTC)
				dest[i] = t

			case "java.sql.Timestamp":
				v, err := r.ReadInt64()
				if err != nil {
					return err
				}
				t := time.Unix(0, v*1000000)
				t = t.In(time.UTC)
				dest[i] = t

			case "java.lang.String":
				v, err := r.ReadString()
				if err != nil {
					return err
				}
				dest[i] = []byte(v)

			case "java.lang.Double":
				v, err := r.ReadFloat64()
				if err != nil {
					return err
				}
				dest[i] = v

			case "java.lang.Float":
				v, err := r.ReadFloat32()
				if err != nil {
					return err
				}
				dest[i] = v

			default:
				panic(r.classes[i])
			}
		}
		r.buffer = append(r.buffer, dest)
	}
	return nil
}

type result struct {
	*driverConnection
	c int64
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
