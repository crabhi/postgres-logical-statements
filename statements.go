// Based on https://github.com/jackc/pglogrepl/blob/70a00e46998bf0f32830278863d5c5e0dd9a0e4d/example/pglogrepl_demo/main.go (MIT licensed)


package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "io"
    "time"
    "fmt"
    "strings"
    "syscall"

    "github.com/jackc/pgconn"
    "github.com/jackc/pglogrepl"
    "github.com/jackc/pgproto3/v2"
    "github.com/jackc/pgtype"
    "github.com/jackc/pgx"
    // "internal/sanitize"
)

type query struct {
    sql string
    args []interface{}
}

type replstate struct {
    relations map[uint32]*pglogrepl.RelationMessage
    to_execute chan query
    connInfo *pgtype.ConnInfo
    signals chan os.Signal
}


func main() {
    var pkColName = "id"  // We always use `id` as the PRIMARY KEY for the tables we want to replicate with this tool.
    const outputPlugin = "pgoutput"
    conn, err := pgconn.Connect(context.Background(), os.Getenv("SOURCE_CONN") + " replication=database")
    if err != nil {
        log.Fatalln("failed to connect to source PostgreSQL server:", err)
    }
    defer conn.Close(context.Background())

    dsn := os.Getenv("SOURCE_CONN")
    conn_config, err := pgx.ParseDSN(dsn)
    if err != nil {
        log.Fatalf("invalid DSN (%s): %s", dsn, err)
    }
    srcConn, err := pgx.Connect(conn_config)
    if err != nil {
        log.Fatalln("failed to connect to source PostgreSQL server:", err, conn_config)
    }
    defer srcConn.Close()

    slotName := os.Getenv("REPLICATION_SLOT")
    if slotName == "" {
        log.Fatalln("Set REPLICATION_SLOT to the slot you want to use")
    }
    publication := os.Getenv("PUBLICATION")
    if publication == "" {
        log.Fatalln("PUBLICATION unset")
    }

    var pluginArguments []string
    pluginArguments = []string{"proto_version '1'", "publication_names '" + publication + "'"}

    sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
    if err != nil {
        log.Fatalln("IdentifySystem failed:", err)
    }
    log.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos, "DBName:", sysident.DBName)

    var replStart pglogrepl.LSN

    if os.Getenv("WITH_COPY") == "true" {
        slot, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{})
        if err != nil {
            log.Fatalln("CreateReplicationSlot failed:", err)
        }
        log.Println("Created replication slot:", slotName, slot)
        replStart, err = pglogrepl.ParseLSN(slot.ConsistentPoint)
        if err != nil {
            log.Fatalln("Bad LSN:", slot.ConsistentPoint)
        }

        rows, err := srcConn.Query("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1", publication)
        if err != nil {
            log.Fatalln("Can't select tables from publication")
        }
        defer rows.Close()

        var tables []pgx.Identifier
        for rows.Next() {
            var schema string
            var table string
            err := rows.Scan(&schema, &table)
            if err != nil {
                log.Fatalln("Error reading row", err)
            }
            tables = append(tables, pgx.Identifier{schema, table})
        }
        if rows.Err() != nil {
            log.Fatalln("Error reading publication tables", err)
        }

        copyTables(tables, slot.SnapshotName)
    } else {
        var restartLsn string
        err = srcConn.QueryRow("SELECT restart_lsn FROM pg_replication_slots WHERE slot_name=$1", slotName).Scan(&restartLsn)
        if err != nil {
            log.Fatalln("failed reading replication slot", slotName, err)
        }
        replStart, err = pglogrepl.ParseLSN(restartLsn)
        if err != nil {
            log.Fatalln("Bad LSN:", restartLsn)
        }
    }

    srcConn.Close()

    err = pglogrepl.StartReplication(context.Background(), conn, slotName, replStart, pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments})
    if err != nil {
        log.Fatalln("StartReplication failed:", err)
    }
    log.Println("Logical replication started on slot", slotName)

    clientXLogPos := replStart
    standbyMessageTimeout := time.Second * 2
    nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)
    state := newReplstate()

    for {
        select {
        case sig := <- state.signals:
            pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
            log.Printf("Sent status update (%s), exiting: %s", clientXLogPos, sig)
            return
        default:
            // pass
        }
        if time.Now().After(nextStandbyMessageDeadline) {
            err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos})
            if err != nil {
                log.Fatalln("SendStandbyStatusUpdate failed:", err)
            }
            // log.Println("Sent Standby status message")
            nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
        }

        ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
        rawMsg, err := conn.ReceiveMessage(ctx)
        cancel()
        if err != nil {
            if pgconn.Timeout(err) {
                continue
            }
            log.Fatalln("ReceiveMessage failed:", err)
        }

        if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
            log.Fatalln("received Postgres WAL error: %+v", errMsg)
        }

        msg, ok := rawMsg.(*pgproto3.CopyData)
        if !ok {
            log.Fatalln("Received unexpected message: %T\n", rawMsg)
        }

        switch msg.Data[0] {
        case pglogrepl.PrimaryKeepaliveMessageByteID:
            pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
            if err != nil {
                log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
            }

            if pkm.ReplyRequested {
                log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)
                nextStandbyMessageDeadline = time.Time{}
            }

        case pglogrepl.XLogDataByteID:
            xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
            if err != nil {
                log.Fatalln("ParseXLogData failed:", err)
            }
            // log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))
            logicalMsg, err := pglogrepl.Parse(xld.WALData)
            if err != nil {
                log.Fatalf("Parse logical replication message: %s", err)
            }
            // log.Printf("Receive a logical replication message: %s", logicalMsg.Type())
            switch logicalMsg := logicalMsg.(type) {
            case *pglogrepl.RelationMessage:
                state.relations[logicalMsg.RelationID] = logicalMsg

            case *pglogrepl.BeginMessage:
                state.Execute("BEGIN", nil)
                log.Println("BEGIN")
                // Indicates the beginning of a group of changes in a transaction. This is only sent for committed transactions. You won't get any events from rolled back transactions.

            case *pglogrepl.CommitMessage:
                state.Execute("COMMIT", nil)
                log.Println("COMMIT")

            case *pglogrepl.InsertMessage:
                rel, ok := state.relations[logicalMsg.RelationID]
                if !ok {
                    log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
                }

                var colnames []string
                var placeholders []string
                var values []interface{}

                for idx, col := range logicalMsg.Tuple.Columns {
                    colName := rel.Columns[idx].Name
                    val, include := parseValue(col, rel.Columns[idx].DataType, state.connInfo, rel)
                    if !include {
                        log.Fatalf("Unexpected unchanged TOAST in INSERT")
                    }

                    colnames = append(colnames, pgx.Identifier{colName}.Sanitize())
                    values = append(values, val)
                    placeholders = append(placeholders, fmt.Sprintf("$%d", idx + 1))
                }

                table := pgx.Identifier{rel.Namespace, rel.RelationName}.Sanitize()
                sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(colnames, ", "), strings.Join(placeholders, ", "))
                state.Execute(sql, values)
                log.Println(sql, ";", values)

            case *pglogrepl.UpdateMessage:
                rel, ok := state.relations[logicalMsg.RelationID]
                if !ok {
                    log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
                }

                table := pgx.Identifier{rel.Namespace, rel.RelationName}.Sanitize()
                var columnAssignments []string
                var values []interface{}

                for idx, col := range logicalMsg.NewTuple.Columns {
                    colName := rel.Columns[idx].Name
                    val, include := parseValue(col, rel.Columns[idx].DataType, state.connInfo, rel)
                    if include {
                        columnAssignments = append(columnAssignments, fmt.Sprintf("%s = $%d", pgx.Identifier{colName}.Sanitize(), idx + 1))
                        values = append(values, val)
                    }
                }

                var idValue interface{}
                var include bool

                switch logicalMsg.OldTupleType {
                case pglogrepl.UpdateMessageTupleTypeNone:
                    for idx := range logicalMsg.NewTuple.Columns {
                        colName := rel.Columns[idx].Name
                        if colName == pkColName {
                            idValue = values[idx]
                            break;
                        }
                    }

                case pglogrepl.UpdateMessageTupleTypeKey:
                    for idx, col := range logicalMsg.OldTuple.Columns {
                        colName := rel.Columns[idx].Name
                        if colName == pkColName {
                            idValue, include = parseValue(col, rel.Columns[idx].DataType, state.connInfo, rel)
                            if !include {
                                log.Fatalf("Unexpected unchanged TOAST in ID column")
                            }

                            break;
                        }
                    }

                default:
                    log.Fatalf("Can only process tables with REPLICA IDENTITY DEFAULT (table %s)", rel)
                }

                values = append(values, idValue)
                sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s = $%d", table, strings.Join(columnAssignments, ", "), pkColName, len(values))

                state.Execute(sql, values)
                log.Println(sql, ";", values)

            case *pglogrepl.DeleteMessage:
                rel, ok := state.relations[logicalMsg.RelationID]
                if !ok {
                    log.Fatalf("unknown relation ID %d", logicalMsg.RelationID)
                }
                table := pgx.Identifier{rel.Namespace, rel.RelationName}.Sanitize()

                if logicalMsg.OldTupleType != pglogrepl.DeleteMessageTupleTypeKey {
                    log.Fatalf("Can only process tables with REPLICA IDENTITY DEFAULT (table %s)", table)
                }

                var idValue interface{}
                var include bool

                for idx, col := range logicalMsg.OldTuple.Columns {
                    if rel.Columns[idx].Name == pkColName {
                        idValue, include = parseValue(col, rel.Columns[idx].DataType, state.connInfo, rel)
                        if !include {
                            log.Fatalf("Unexpected unchanged TOAST in ID column")
                        }
                        break
                    }
                }

                if idValue == nil {
                    log.Fatalf("No value for column %s", pkColName)
                }

                sql := fmt.Sprintf("DELETE FROM %s WHERE %s = $1", table, pkColName)
                state.Execute(sql, []interface{}{idValue})
                log.Println(sql, ";", []interface{}{idValue})

            case *pglogrepl.TruncateMessage:
                for _, relid := range logicalMsg.RelationIDs {
                    rel, ok := state.relations[relid]
                    if !ok {
                        log.Fatalf("unknown relation ID %d", relid)
                    }
                    table := pgx.Identifier{rel.Namespace, rel.RelationName}.Sanitize()
                    log.Printf("TRUNCATE %s;\n", table)
                }

            case *pglogrepl.TypeMessage:
                log.Printf("%v", logicalMsg)
            case *pglogrepl.OriginMessage:
                log.Printf("%v", logicalMsg)
            default:
                log.Printf("Unknown message type in pgoutput stream: %T", logicalMsg)
            }

            clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
        }
    }
}

func execute(c chan query) {
    dsn := os.Getenv("DEST_CONN")
    conn_config, err := pgx.ParseDSN(dsn)
    if err != nil {
        log.Fatalf("invalid DSN (%s): %s", dsn, err)
    }
    conn, err := pgx.Connect(conn_config)
    if err != nil {
        log.Fatalln("failed to connect to dest PostgreSQL server:", err)
    }

    var tx *pgx.Tx

    for q := range c {
        switch q.sql {
        case "BEGIN":
            if tx != nil {
                log.Fatalln("BEGIN in already open transaction")
            }
            tx, err = conn.Begin()
            if err != nil {
                log.Fatalln("can't create transaction", err)
            }
            defer tx.Rollback()
        case "COMMIT":
            err = tx.Commit()
            if err != nil {
                log.Fatalln("Can't commit transaction", err)
            }
            tx = nil
        default:
            _, err = tx.Exec(q.sql, q.args...)
            if err != nil {
                log.Fatalf("Can't execute query %#v: %s", q, err)
            }
        }
    }
}

func newReplstate() replstate {
    c := make(chan query, 2)

    go execute(c)

    s := replstate{}
    s.relations = make(map[uint32]*pglogrepl.RelationMessage)
    s.to_execute = c
    s.connInfo = pgtype.NewConnInfo()
    s.signals = make(chan os.Signal, 1)

    signal.Notify(s.signals, syscall.SIGINT, syscall.SIGTERM)
    return s
}


func (s *replstate) Execute(sql string, args []interface{}) {
    s.to_execute <- query{sql, args}
}

func parseValue(col *pglogrepl.TupleDataColumn, dataType uint32, connInfo *pgtype.ConnInfo, rel *pglogrepl.RelationMessage) (interface{}, bool) {
    switch col.DataType {
        case 'n': // null
        return nil, true
        case 'u': // unchanged toast
        // This TOAST value was not changed. TOAST values are not stored in the tuple, and logical replication doesn't want to spend a disk read to fetch its value for you.
        return nil, false
        case 't': //text
        val, err := decodeTextColumnData(connInfo, col.Data, dataType)
        if err != nil {
            log.Fatalf("error decoding column data: %w", err)
        }
        return val, true
    default:
        log.Fatalf("unknown datatype %s in %#v", col.DataType, col)
    }
    // should be unreachable
    return "zzzz", true
}

func decodeTextColumnData(ci *pgtype.ConnInfo, data []byte, dataType uint32) (interface{}, error) {
    var decoder pgtype.TextDecoder
    if dt, ok := ci.DataTypeForOID(dataType); ok {
        decoder, ok = dt.Value.(pgtype.TextDecoder)
        if !ok {
            decoder = &pgtype.GenericText{}
        }
    } else {
        decoder = &pgtype.GenericText{}
    }
    if err := decoder.DecodeText(ci, data); err != nil {
        return nil, err
    }
    return decoder.(pgtype.Value).Get(), nil
}

func copyTables(tables []pgx.Identifier, snapshotName string) error {
    results := make(chan error)
    for _, table := range tables {
        go copyTable(table, snapshotName, results)
    }

    for range tables {
        err := <-results
        if err != nil {
            return err
        }
    }
    return nil
}

func copyTable(table pgx.Identifier, snapshotName string, results chan error) {
    dsn := os.Getenv("SOURCE_CONN")
    conn_config, err := pgx.ParseDSN(dsn)
    if err != nil {
        log.Fatalf("invalid DSN (%s): %s", dsn, err)
    }
    srcConn, err := pgx.Connect(conn_config)
    if err != nil {
        log.Fatalln("failed to connect to source PostgreSQL server:", err, conn_config)
    }
    defer srcConn.Close()

    dsn = os.Getenv("DEST_CONN")
    conn_config, err = pgx.ParseDSN(dsn)
    if err != nil {
        log.Fatalf("invalid DSN (%s): %s", dsn, err)
    }
    dstConn, err := pgx.Connect(conn_config)
    if err != nil {
        log.Fatalln("failed to connect to dest PostgreSQL server:", err, conn_config)
    }
    defer dstConn.Close()

    rows, err := srcConn.Query("SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2", table[0], table[1])
    if err != nil {
        log.Fatalln("Can't select columns from", table, err)
    }
    defer rows.Close()

    var columns []string
    for rows.Next() {
        var column string
        err := rows.Scan(&column)
        if err != nil {
            log.Fatalln("Error reading row", err)
        }
        columns = append(columns, pgx.Identifier{column}.Sanitize())
    }
    if rows.Err() != nil {
        log.Fatalln("Error reading column names", err)
    }

    tx, err := srcConn.Begin()
    if err != nil {
        log.Fatalln("can't create transaction", err)
    }

    _, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    if err != nil {
        log.Fatalln("can't create transaction", err)
    }
    _, err = tx.Exec("SET TRANSACTION SNAPSHOT '" + snapshotName + "'")  // can't pass as parameter for some reason
    if err != nil {
        log.Fatalln("can't set transaction snapshot", err)
    }

    r, w := io.Pipe()
    go func() {
        defer w.Close()

        _, err := tx.CopyToWriter(w, fmt.Sprintf("COPY (SELECT * FROM %s) TO STDOUT", table.Sanitize()))
        if err != nil {
            log.Fatalln("Can't copy from table", err)
        }
    }()

    _, err = dstConn.Exec("TRUNCATE TABLE " + table.Sanitize())
    if err != nil {
        log.Fatalln("can't truncate", err)
    }
    rowsCopied, err := dstConn.CopyFromReader(r, fmt.Sprintf("COPY %s FROM STDIN", table.Sanitize()))
    if err != nil {
        log.Fatalln("can't copy from", err)
    }

    log.Printf("Copy finished - table %s (%s)", table, rowsCopied)
    results <- nil
}
