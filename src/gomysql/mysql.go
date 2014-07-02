package gomysql

import (
    "bufio"
    "time"
    "net"
    "fmt"
    "io"
)

// infomation from handshake packet
type serverInfo struct {
    protocol_version    byte        // 协议版本号(0x0a)
    server_version      []byte      // 服务器版本信息
    connection_id       uint32      // 连接ID(show processlist显示的id)
    scramble            [20]byte    // Server端认证方法产生的认证数据
    capability          uint16      // Server支持的特性
    lang                byte        // Server的字符集
}

// MySQL connection handler
type MySQL struct {
    protocol        string      // Network protocol
    address         string      // Server address
    user            string      // MySQL username
    passwd          string      // MySQL password
    dbname          string      // Database name

    socket          net.Conn    // MySQL connection
    rd              *bufio.Reader
    wr              *bufio.Writer

    info            serverInfo  // MySQL server information
    seq             byte        // MySQL sequence number
    unreaded_reply  bool
    status          uint16      // Current status of MySQL server connection
    max_pkt_size    int         // Maximum packet size that client can accept from server
    timeout         time.Duration // Timeout for connect
    narrowTypeSet   bool        // Return only types accepted by godrv
    fullFieldInfo   bool        // Store full information about fields in result
    Debug           bool        // Debug logging. You may change it at any time.
}

func Open(protocol, address, user, passwd, dbname string) (*MySQL, error) {
    my := &MySQL {
        protocol:      protocol,
        address:       address,
        user:          user,
        passwd:        passwd,
        dbname:        dbname,
        max_pkt_size:  16*1024*1024 - 1,
        timeout:       2 * time.Minute,
        fullFieldInfo: true,
    };

    var err error;
    my.socket, err = net.Dial(protocol, address);
    if err != nil { return nil, err; }

    // Enable TCP Keepalives on TCP connections
    if tc, ok := my.socket.(*net.TCPConn); ok {
        if err := tc.SetKeepAlive(true); err != nil {
            tc.Close();
            return nil, err;
        }
    }

    my.rd = bufio.NewReader(my.socket);
    my.wr = bufio.NewWriter(my.socket);

    // Initialisation
    my.init()
    my.auth()

    res := my.getResult(nil, nil)
    if res == nil {
        // Try old password
        my.oldPasswd()
        res = my.getResult(nil, nil)
        if res == nil {
            return nil, ErrAuthentication
        }
    }

    return my, nil;
}

// Close connection to the server
func (my *MySQL) Close() (err error) {
    if my.socket == nil {
        return ErrNotConn
    }
    if my.unreaded_reply {
        return ErrUnreadedReply
    }

    return my.closeConn()
}

// Exec a new query.
//
// If you specify the parameters, the SQL string will be a result of
// fmt.Sprintf(sql, params...).
// You must get all result rows (if they exists) before next query.
func (my *MySQL) Query(sql string, params ...interface{}) (res *Result, err error) {
    defer catchError(&err)

    if my.socket == nil { return nil, ErrNotConn; }
    if my.unreaded_reply { return nil, ErrUnreadedReply; }

    if len(params) != 0 {
        sql = fmt.Sprintf(sql, params...)
    }

    // Send query
    my.sendCmdStr(_COM_QUERY, sql)

    // Get command response
    res = my.getResponse()
    return res, nil;
}

// Get the data row from server. This method reads one row of result set
// directly from network connection (without rows buffering on client side).
// Returns io.EOF if there is no more rows in current result set.
func (res *Result) ScanRow(row Row) error {
    if row == nil {
        return ErrRowLength
    }
    if res.eor_returned {
        return ErrReadAfterEOR
    }
    if res.StatusOnly() {
        // There is no fields in result (OK result)
        res.eor_returned = true
        return io.EOF
    }
    err := res.getRow(row)
    if err == io.EOF {
        res.eor_returned = true
        if !res.MoreResults() {
            res.my.unreaded_reply = false
        }
    }
    return err
}

// Returns true if more results exixts. You don't have to call it before
// NextResult method (NextResult returns nil if there is no more results).
func (res *Result) MoreResults() bool {
    return res.status&_SERVER_MORE_RESULTS_EXISTS != 0
}

/******************************************************************************
*                           Private Functions                                 *
******************************************************************************/

func (my *MySQL) closeConn() (err error) {
    defer catchError(&err)

    // Always close and invalidate connection, even if
    // COM_QUIT returns an error
    defer func() {
        err = my.socket.Close()
        my.socket = nil // Mark that we disconnect
    }()

    // Close the connection
    my.sendCmd(_COM_QUIT)
    return
}

func (my *MySQL) getResponse() (res *Result) {
    res = my.getResult(nil, nil)
    if res == nil {
        panic(ErrBadResult)
    }
    my.unreaded_reply = !res.StatusOnly()
    return res;
}
