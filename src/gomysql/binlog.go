package gomysql

import (
    "strconv"
    "time"
    "math"
    "fmt"
)

func isSet(bits []byte, index uint) bool {
    return bits[index / 8] & (1 << (index % 8)) != 0
}

func (my *MySQL) Listen(start uint32, flags uint16, slave_id uint32, file string) {
    // Reset sequence number
    my.seq = 0

    pay_len := 1 + 4 + 2 + 4
    if len(file) > 0 {
        pay_len += len(file) + 1
    }

    pw := my.newPktWriter(pay_len)
    pw.writeByte(_COM_BINLOG_DUMP)
    pw.writeU32(start) // Start position
    pw.writeU16(flags) // Flags
    pw.writeU32(slave_id) // Slave server id
    if len(file) > 0 {
        pw.writeNT(file)
    }
}

func (my *MySQL) NextEvent() *pktReader {
    pr := my.newPktReader()

    // skip 00
    ok := pr.readByte();
    if ok == 0xff { pr.skipAll(); pr = nil; return nil; }
    if ok == 0xfe { pr.skipAll(); pr = nil; return nil; }
    return pr;
}

func (my *MySQL) GetEventHeader(pr *pktReader) *EventHeader {
    eh := new(EventHeader);
    eh.Timestamp = pr.readU32();
    eh.EventType = pr.readByte();
    eh.Serverid = pr.readU32();
    eh.TotalSize = pr.readU32();
    eh.MasterPosition = pr.readU32();
    eh.Flag1 = pr.readByte();
    eh.Flag2 = pr.readByte();
    return eh;
}

func (my *MySQL) DestroyEvent(pr *pktReader) {
    pr.skipAll();
    pr = nil;
}

func (my *MySQL)ParseFormatDescriptionEvent(eh *EventHeader, pr *pktReader) *FormatDescriptionEvent {
    event := new(FormatDescriptionEvent);
    event.Header = *eh;
    event.BinlogVersion = pr.readU16();

    buffer := make([]byte, 50);
    pr.readFull(buffer);
    event.ServerVersion = string(buffer);

    event.CreateTimestamp = pr.readU32();
    event.EventHeaderLength = pr.readByte();
    event.EventTypeHeaderLengths = pr.readAll();

    return event;
}

func (my *MySQL)ParseQueryEvent(eh *EventHeader, pr *pktReader) *QueryEvent {
    event := new(QueryEvent);
    event.Header = *eh;
    event.SlaveProxyId = pr.readU32();
    event.ExecutionTime = pr.readU32();

    schemaname_len := pr.readByte();

    event.ErrorCode = pr.readU16();

    vars_len := pr.readU16();
    buffer := make([]byte, vars_len);
    pr.readFull(buffer);
    event.StatusVars = string(buffer);
    buffer = nil;

    buffer = make([]byte, schemaname_len);
    pr.readFull(buffer);
    event.SchemaName = string(buffer);
    buffer = nil;

    pr.skipN(1);
    event.Query = string(pr.readAll());

    return event;
}

func (my *MySQL)ParseTableMapEvent(eh *EventHeader, pr *pktReader) *TableMapEvent {
    event := new(TableMapEvent);
    event.Header = *eh;

    buffer := make([]byte, 6);
    pr.readFull(buffer);
    event.TableID = DecodeU64(buffer);
    buffer = nil;

    event.Flags = pr.readU16();

    len := pr.readByte();
    buffer = make([]byte, len);
    pr.readFull(buffer);
    event.SchemaName = string(buffer);
    buffer = nil;

    pr.skipN(1);

    len = pr.readByte();
    buffer = make([]byte, len);
    pr.readFull(buffer);
    event.TableName = string(buffer);
    buffer = nil;

    pr.skipN(1);

    event.ColumnCount = pr.readLCB();

    event.ColumnTypes = make([]byte, event.ColumnCount);
    pr.readFull(event.ColumnTypes);

    pr.readLCB();
    event.ColumnMeta = make([][]byte, event.ColumnCount);

    for i := 0; i < (int)(event.ColumnCount); i++ {
        if TYPE_META_LEN[int(event.ColumnTypes[i])] == 0 { 
            continue; 
        }
        
        event.ColumnMeta[i] = make([]byte, TYPE_META_LEN[int(event.ColumnTypes[i])]);
        pr.readFull(event.ColumnMeta[i]);
    }

    event.NullBitmap = make([]byte, int((event.ColumnCount +8)/7));
    pr.readFull(event.NullBitmap);

    return event;
}

func (my *MySQL)ParseRowsEvent(eh *EventHeader, pr *pktReader, tablemap *map[uint64]*TableMapEvent) *RowsEvent {
    event := new(RowsEvent);
    event.Header = *eh;

    buffer := make([]byte, 6);
    pr.readFull(buffer);
    event.TableID = DecodeU64(buffer);
    buffer = nil;

    tme, _ := (*tablemap)[event.TableID];

    event.Flags = pr.readU16();

    event.ExtraDataLen = pr.readU16();
    if event.ExtraDataLen - 2 > 0 {
        event.ExtraData = make([]byte, event.ExtraDataLen);
        pr.readFull(event.ExtraData);
    }

    event.ColumnCount = pr.readLCB();

    event.ColumnsPresentBitmap1 = make([]byte, int((event.ColumnCount +8)/7));
    pr.readFull(event.ColumnsPresentBitmap1);

    if eh.EventType == UPDATE_ROWS_EVENT {
        event.ColumnsPresentBitmap2 = make([]byte, int((event.ColumnCount +8)/7));
        pr.readFull(event.ColumnsPresentBitmap2);
    }

    i := 0;
    for !pr.eof() {
        nullBitmap := make([]byte, (event.ColumnCount+7)/8);
        pr.readFull(nullBitmap);
        event.NullBitmap = append(event.NullBitmap, nullBitmap);
        
        row := make([]interface{}, event.ColumnCount);
        for j := 0; j < int(event.ColumnCount); j++ {
            if isSet(nullBitmap, uint(j)) {
                row[j] = nil;
                continue;
            }
            switch tme.ColumnTypes[j] {
            case MYSQL_TYPE_NULL:
                row[j] = nil;
            case MYSQL_TYPE_TINY:
                row[j] = pr.readByte();
            case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
                row[j] = pr.readU16();
            case MYSQL_TYPE_INT24:
                row[j] = pr.readU24();
            case MYSQL_TYPE_LONG:
                row[j] = pr.readU32();
            case MYSQL_TYPE_LONGLONG:
                row[j] = pr.readU64();
            case MYSQL_TYPE_FLOAT:
                row[j] = float64(math.Float32frombits(pr.readU32()));
            case MYSQL_TYPE_DOUBLE:
                row[j] = math.Float64frombits(pr.readU64());
            case MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_BIT, MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_SET, MYSQL_TYPE_ENUM, MYSQL_TYPE_GEOMETRY:
                row[j] = pr.readBin();
            case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
                row[j], _ = strconv.ParseFloat(string(pr.readBin()), 64);
            case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
                row[j] = pr.readDate();
            case MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
                row[j] = pr.readTime();
            case MYSQL_TYPE_TIME:
                row[j] = pr.readDuration()
            }
        }
        event.Rows = append(event.Rows, row);
        i++;
    }
    return event;
}

func PrintEventHeader(eh *EventHeader) {
    fmt.Println("\n================ EventHeader =====================");
    fmt.Printf("Timestamp           : %v\n", time.Unix(int64(eh.Timestamp), 0));
    fmt.Printf("Event Type          : %v\n", EVENT_DESC[eh.EventType]);
    fmt.Printf("Server ID           : %v\n", eh.Serverid);
    fmt.Printf("TotalSize           : %v\n", eh.TotalSize);
    fmt.Printf("Master Position     : %v\n", eh.MasterPosition);
    fmt.Printf("Flag1               : %v\n", eh.Flag1);
    fmt.Printf("Flag2               : %v\n", eh.Flag2);
}

func PrintFormatDescriptionEvent(fde *FormatDescriptionEvent) error {
    PrintEventHeader(&fde.Header);
    fmt.Println("..................................................");
    fmt.Printf("Binlog Version      : %v\n", fde.BinlogVersion);
    fmt.Printf("Server Version      : %v\n", fde.ServerVersion);
    fmt.Printf("Timestamp           : %v\n", time.Unix(int64(fde.CreateTimestamp), 0));
    fmt.Printf("EventHeader Length  : %v\n", fde.EventHeaderLength);
    fmt.Printf("EventTypeHeader Lengths : %v\n", fde.EventTypeHeaderLengths);
    fmt.Printf("\n\n");
    return nil;
}

func PrintQueryEvent(qe *QueryEvent) error {
    PrintEventHeader(&qe.Header);
    fmt.Println("..................................................");
    fmt.Printf("Slave Proxy ID      : %v\n", qe.SlaveProxyId);
    fmt.Printf("Execution Time      : %v\n", time.Unix(int64(qe.ExecutionTime), 0));
    fmt.Printf("Schema Name         : %s\n", qe.SchemaName);
    // fmt.Printf("Status Vars         : %s\n", qe.StatusVars);
    fmt.Printf("Query               : %s\n", qe.Query);
    fmt.Printf("\n\n");
    return nil;
}

func PrintTableMapEvent(tme *TableMapEvent) error {
    PrintEventHeader(&tme.Header);
    fmt.Println("..................................................");
    fmt.Printf("Table ID            : %d\n", tme.TableID);
    fmt.Printf("Flags               : %d\n", tme.Flags);
    fmt.Printf("Schema Name         : %s\n", tme.SchemaName);
    fmt.Printf("Table Name          : %s\n", tme.TableName);
    fmt.Printf("Column Count        : %d\n", tme.ColumnCount);
    fmt.Printf("Column Types        : %v\n", tme.ColumnTypes);
    fmt.Printf("Column Meta         : %v\n", tme.ColumnMeta);
    fmt.Printf("Null Bitmap         : %v\n", tme.NullBitmap);
    fmt.Printf("\n\n");
    return nil;
}

func PrintRowsEvent(re *RowsEvent) error {
    PrintEventHeader(&re.Header);
    fmt.Println("..................................................");
    fmt.Printf("Table ID            : %d\n", re.TableID);
    fmt.Printf("Extra Data Length   : %d\n", re.ExtraDataLen);
    fmt.Printf("Extra Data          : %v\n", re.ExtraData);
    fmt.Printf("Flags               : %d\n", re.Flags);
    fmt.Printf("Column Count        : %d\n", re.ColumnCount);
    fmt.Printf("ColumnPresentBitmap1: %v\n", re.ColumnsPresentBitmap1);
    if re.Header.EventType == UPDATE_ROWS_EVENT {
        fmt.Printf("ColumnPresentBitmap2: %v\n", re.ColumnsPresentBitmap2);
    }
    fmt.Printf("Null Bitmap         : %v\n", re.NullBitmap);
    fmt.Printf("Rows                : %v\n", re.Rows);
    fmt.Printf("\n\n");
    return nil;
}