package gomysql

import (
)

// binlog event header
type EventHeader struct {
    Timestamp       uint32;
    EventType       byte;
    Serverid        uint32; 
    TotalSize       uint32;
    MasterPosition  uint32;
    Flag1           byte;
    Flag2           byte;
}

type GenericEvent struct {
    Header          EventHeader;
    body            []byte;
}

type FormatDescriptionEvent struct {    
    Header                  EventHeader;
    BinlogVersion           uint16
    ServerVersion           string
    CreateTimestamp         uint32
    EventHeaderLength       uint8
    EventTypeHeaderLengths  []uint8
}

type QueryEvent struct {
    Header          EventHeader
    SlaveProxyId    uint32
    ExecutionTime   uint32
    ErrorCode       uint16
    SchemaName      string
    StatusVars      string
    Query           string
}

type TableMapEvent struct {
    Header      EventHeader
    TableID     uint64
    Flags       uint16
    SchemaName  string
    TableName   string
    ColumnCount uint64
    ColumnTypes []byte
    ColumnMeta  [][]byte
    NullBitmap  []byte
}

type RowsEvent struct {
    Header          EventHeader
    TableID         uint64
    ExtraDataLen    uint16
    ExtraData       []byte
    Flags           uint16
    ColumnCount     uint64
    ColumnsPresentBitmap1 []byte
    ColumnsPresentBitmap2 []byte
    NullBitmap      [][]byte
    Rows            [][]interface{}
}

