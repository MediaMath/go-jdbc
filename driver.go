package gojdbc

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"
)

const (
	_           = iota
	commandDone = iota
	commandPrepare
	commandSetlong
	commandSetstring
	commandExecute
	commandNext
	commandGet
	commandSetdouble
	commandCloseStatement
	commandCloseResultSet
	commandBeginTransaction
	commandCommitTransaction
	commandRollbackTransaction
	commandSettime
	commandSetnull
	commandSetquerytimeout

	commandCloseConnection = -1
	commandServerStatus    = 254
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
	paramTimeout         = "timeout"
	paramQueryTimeout    = "queryTimeout"
	connectionTestString = "d67c184ff3c42e7b7a0bf2d4bca50340"
	bufferSize           = 1000
)

func ServerStatus(name string) (string, error) {
	u, e := url.Parse(name)
	if e != nil {
		return "", e
	}
	var newConnection net.Conn

	if timeoutRaw, ok := u.Query()[paramTimeout]; ok && len(timeoutRaw) > 0 {
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
		return "", e
	}

	defer newConnection.Close()

	dc := &driverConnection{conn: newConnection}
	if s, e := dc.ReadString(); e != nil {
		return "", e
	} else if s != connectionTestString {
		return "", fmt.Errorf("Connection test failed, %s != %s", s, connectionTestString)
	}

	if e := dc.WriteByte(commandServerStatus); e != nil {
		return "", e
	}

	return dc.ReadString()
}

func (j Driver) Open(name string) (driver.Conn, error) {
	u, e := url.Parse(name)
	if e != nil {
		return nil, e
	}

	var newConnection net.Conn

	if timeoutRaw, ok := u.Query()[paramTimeout]; ok && len(timeoutRaw) > 0 {
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

	var queryTimeout int64
	if timeoutRaw, ok := u.Query()[paramQueryTimeout]; ok && len(timeoutRaw) > 0 {
		qt, e := strconv.ParseInt(timeoutRaw[0], 10, 64)
		if e != nil {
			return nil, e
		}
		queryTimeout = qt
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
	} else if s != connectionTestString {
		newConnection.Close()
		return nil, fmt.Errorf("Connection test failed, %s != %s", s, connectionTestString)
	}

	return &conn{openStmt: map[string]*stmt{}, dc: dc, queryTimeout: queryTimeout}, nil
}

type conn struct {
	openStmt     map[string]*stmt
	openStmtLock sync.Mutex
	dc           *driverConnection
	tx           *tx
	queryTimeout int64
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if e := c.dc.WriteByte(commandPrepare); e != nil {
		return nil, e
	}

	id, _ := NewV4()
	if e := c.dc.WriteString(id.String()); e != nil {
		return nil, e
	}

	if e := c.dc.WriteString(query); e != nil {
		return nil, e
	}

	if errMsg, e := c.dc.CheckError(); e != nil {
		return nil, e
	} else if errMsg != "" {
		return nil, fmt.Errorf(errMsg)
	}

	if c.queryTimeout > 0 {
		var e error
		if e = c.dc.WriteByte(commandSetquerytimeout); e == nil {
			if e = c.dc.WriteString(id.String()); e == nil {
				if e = c.dc.WriteInt64(c.queryTimeout); e == nil {
					var errMsg string
					if errMsg, e = c.dc.CheckError(); e == nil && errMsg != "" {
						return nil, fmt.Errorf(errMsg)
					}
				}
			}
		}
		if e != nil {
			return nil, e
		}
	}

	s := &stmt{conn: c, id: id.String(), query: query}
	c.openStmtLock.Lock()
	defer c.openStmtLock.Unlock()
	c.openStmt[s.id] = s
	return s, nil
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
	if e := c.dc.WriteByte(11); e != nil {
		return nil, e
	}
	return c.tx, nil

}

type tx struct {
	c        *conn
	finished bool
}

// return false if previously finished, don't finished twice
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

	if msg, e := t.c.dc.CheckError(); e != nil {
		return e
	} else if msg != "" {
		return fmt.Errorf(msg)
	}

	return nil
}

func (t *tx) Rollback() error {
	if !t.finish() {
		return nil
	}
	if e := t.c.dc.WriteByte(13); e != nil {
		return e
	}

	if msg, e := t.c.dc.CheckError(); e != nil {
		return e
	} else if msg != "" {
		return fmt.Errorf(msg)
	}

	return nil
}

type stmt struct {
	conn           *conn
	id             string
	query          string
	closed         bool
	stmtCloseMutex sync.Mutex
}

// TODO: Do not close if in use by any queries
func (s *stmt) Close() error {
	s.stmtCloseMutex.Lock()
	defer s.stmtCloseMutex.Unlock()
	if !s.closed {
		defer s.conn.putStmt(s)
		s.closed = true
		if e := s.conn.dc.WriteByte(commandCloseStatement); e != nil {
			return e
		}

		if e := s.conn.dc.WriteString(s.id); e != nil {
			return e
		}

		if msg, e := s.conn.dc.CheckError(); e != nil {
			return e
		} else if msg != "" {
			return fmt.Errorf(msg)
		}

	}
	return nil
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	var (
		b byte
		e error
	)

	if e = s.stage1(args); e == nil {
		if e = s.conn.dc.WriteByte(commandExecute); e == nil {
			if e = s.conn.dc.WriteString(s.id); e == nil {
				b, e = s.conn.dc.ReadByte()
			}
		}
	}

	if e != nil {
		return nil, e
	}

	switch b {
	case 0:
		var c int32

		if s.conn.tx == nil {
			if c, e = s.conn.dc.ReadInt32(); e != nil {
				return nil, e
			} else if c < 0 {
				c = 0
			}
		}

		return result{s.conn.dc, int64(c)}, nil

	case 2:
		errMsg, e := s.conn.dc.ReadString()
		if e != nil {
			return nil, e
		}
		return nil, errors.New(errMsg)
	default:
		log.Println("Did not expect %d for exec", b)
		return nil, driver.ErrBadConn
	}
}

func (s *stmt) stage1(args []driver.Value) (e error) {
	for i, x := range args {
		var e error
		switch xT := x.(type) {
		case int64:
			if e = s.conn.dc.WriteByte(commandSetlong); e == nil {
				if e = s.conn.dc.WriteString(s.id); e == nil {
					if e = s.conn.dc.WriteInt32(int32(i + 1)); e == nil {
						e = s.conn.dc.WriteInt64(xT)
					}
				}
			}
		case string:
			if e = s.conn.dc.WriteByte(commandSetstring); e == nil {
				if e = s.conn.dc.WriteString(s.id); e == nil {
					if e = s.conn.dc.WriteInt32(int32(i + 1)); e == nil {
						e = s.conn.dc.WriteString(xT)
					}
				}
			}
		case float64:
			if e = s.conn.dc.WriteByte(commandSetdouble); e == nil {
				if e = s.conn.dc.WriteString(s.id); e == nil {
					if e = s.conn.dc.WriteInt32(int32(i + 1)); e == nil {
						e = s.conn.dc.WriteFloat64(xT)
					}
				}
			}
		case time.Time:
			if e = s.conn.dc.WriteByte(commandSettime); e == nil {
				if e = s.conn.dc.WriteString(s.id); e == nil {
					if e = s.conn.dc.WriteInt32(int32(i + 1)); e == nil {
						e = s.conn.dc.WriteInt64(xT.UnixNano() / 1000000)
					}
				}
			}
		case nil:
			if e = s.conn.dc.WriteByte(commandSetnull); e == nil {
				if e = s.conn.dc.WriteString(s.id); e == nil {
					e = s.conn.dc.WriteInt32(int32(i + 1))
				}
			}
		default:
			return fmt.Errorf("Unhandled param type(%d): %T %v %T %v\n", i, x, x, xT, xT)
		}
		if e != nil {
			return e
		}
	}
	return
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	var (
		b byte
		e error
	)
	if e = s.stage1(args); e == nil {
		if e = s.conn.dc.WriteByte(commandExecute); e == nil {
			if e = s.conn.dc.WriteString(s.id); e == nil {
				b, e = s.conn.dc.ReadByte()
			}
		}
	}
	if e != nil {
		return nil, e
	}

	switch b {
	// No row
	case 0:
		if _, e := s.conn.dc.ReadInt32(); e != nil {
			return nil, e
		}
		return &norows{}, nil

	// Results
	case 1:
		var names, classes []string
		id2, _ := NewV4()

		if e := s.conn.dc.WriteString(id2.String()); e != nil {
			return nil, e
		}

		columns, e := s.conn.dc.ReadInt32()
		if e != nil {
			return nil, e
		}
		for i := 0; i < int(columns); i++ {
			name, e := s.conn.dc.ReadString()
			if e != nil {
				return nil, e
			}

			class, e := s.conn.dc.ReadString()
			if e != nil {
				return nil, e
			}
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
		if errMsg, e := s.conn.dc.ReadString(); e != nil {
			return nil, e
		} else {
			return nil, errors.New(errMsg)
		}
	default:
		log.Println("Unexpected response from driver connection %d", b)
		return nil, driver.ErrBadConn
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
		if e = r.WriteByte(commandCloseResultSet); e == nil {
			e = r.WriteString(r.id)
		}
		if e != nil {
			return e
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
	r.buffer = make([][]driver.Value, 0, bufferSize)
	r.currentRow = 0
	nameLength := len(r.names)

	var e error
	if e = r.WriteByte(commandNext); e == nil {
		if e = r.WriteInt32(int32(bufferSize)); e == nil {
			e = r.WriteString(r.id)
		}
	}
	if e != nil {
		return e
	}

	for i := 0; i < bufferSize; i++ {
		if b, e := r.ReadByte(); e != nil {
			return e
		} else if b == 0 {
			r.hasMore = false
			break
		} else if b == 2 {
			if errMsg, e := r.ReadString(); e != nil {
				return e
			} else {
				return fmt.Errorf(errMsg)
			}

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
				dest[i] = v

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
				return fmt.Errorf("Unexpected class type %s", r.classes[i])
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
