package main

import (
    "gomysql"
    "fmt"
    // "io"
)

func main() {
    db, err := gomysql.Open("tcp", "127.0.0.1:33066", "root", "", "test");
    if err != nil {
        fmt.Printf("connect mysql failed, err: %v\n", err);
        return;
    }

    defer db.Close();
    /*
    res, err := db.Query("select * from tbl");
    if err != nil {
        fmt.Printf("connect mysql failed, err: %v\n", err);
        return;
    }

    row := res.MakeRow()
    for {
        err = res.ScanRow(row)
        if err == io.EOF {
            // No more rows
            break
        }
        if err != nil {
            fmt.Printf("get row failed, err: %v\n", err);
            return;
        }

        // Print all cols
        fmt.Printf("id: %d, name: %s\n", row.Int(0), row.Str(1)); 
    }
    */

    _, err = db.Query("set @master_binlog_checksum= @@global.binlog_checksum");
    if err != nil {
        fmt.Printf("set binlog checksum failed, err: %v\n", err);
        return;
    }

    table_maps := make(map[uint64]*gomysql.TableMapEvent);

    db.Listen(4, 0x01, 23, "mysql-bin.000004");
    for {
        event := db.NextEvent();
        if event == nil { break; }

        eh := db.GetEventHeader(event);

        switch eh.EventType {
        case gomysql.FORMAT_DESCRIPTION_EVENT:
            fde := db.ParseFormatDescriptionEvent(eh, event);
            if fde == nil { break; }
            gomysql.PrintFormatDescriptionEvent(fde);
            fde = nil;
        case gomysql.QUERY_EVENT:
            qe := db.ParseQueryEvent(eh, event);
            if qe == nil { break; }
            gomysql.PrintQueryEvent(qe);
            qe = nil;
        case gomysql.TABLE_MAP_EVENT:
            tme := db.ParseTableMapEvent(eh, event);
            if tme == nil { break; }
            gomysql.PrintTableMapEvent(tme);

            if _, ok := table_maps[tme.TableID]; !ok {
                table_maps[tme.TableID] = tme;
            } else {
                tme = nil;
            }
        case gomysql.WRITE_ROWS_EVENT, gomysql.UPDATE_ROWS_EVENT, gomysql.DELETE_ROWS_EVENT:
            re := db.ParseRowsEvent(eh, event, &table_maps);
            if re == nil { break; }
            gomysql.PrintRowsEvent(re);
            re = nil;
        }
        db.DestroyEvent(event);
        eh = nil;
    }
}

