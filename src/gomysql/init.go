package gomysql

import (
    // "log"
)

func (my *MySQL) init() {
    my.seq = 0; // Reset sequence number, mainly for reconnect
    my.debug("[%2d ->] Init packet:", my.seq);

    pr := my.newPktReader();
    my.info.protocol_version = pr.readByte();
    my.info.server_version = pr.readNTB();
    my.info.connection_id = pr.readU32();
    pr.readFull(my.info.scramble[0:8]);
    pr.skipN(1);
    my.info.capability = pr.readU16();
    my.info.lang = pr.readByte();
    my.status = pr.readU16();
    pr.skipN(13);
    if my.info.capability&_CLIENT_PROTOCOL_41 != 0 {
        pr.readFull(my.info.scramble[8:]);
    }
    pr.skipAll(); // Skip other information
    my.debug(tab8s+"ProtVer=%d, ServVer=\"%s\" Status=0x%x",
            my.info.protocol_version, my.info.server_version, my.status);
    if my.info.capability&_CLIENT_PROTOCOL_41 == 0 {
        panic(ErrOldProtocol)
    }
}

func (my *MySQL) auth() {
    my.debug("[%2d <-] Authentication packet", my.seq);
    flags := uint32(
        _CLIENT_PROTOCOL_41 |
            _CLIENT_LONG_PASSWORD |
            _CLIENT_LONG_FLAG |
            _CLIENT_TRANSACTIONS |
            _CLIENT_SECURE_CONN |
            _CLIENT_LOCAL_FILES |
            _CLIENT_MULTI_STATEMENTS |
            _CLIENT_MULTI_RESULTS)
    // Reset flags not supported by server
    flags &= uint32(my.info.capability) | 0xffff0000
    scrPasswd := encryptedPasswd(my.passwd, my.info.scramble[:])
    pay_len := 4 + 4 + 1 + 23 + len(my.user) + 1 + 1 + len(scrPasswd)
    if len(my.dbname) > 0 {
        pay_len += len(my.dbname) + 1
        flags |= _CLIENT_CONNECT_WITH_DB
    }
    pw := my.newPktWriter(pay_len)
    pw.writeU32(flags)
    pw.writeU32(uint32(my.max_pkt_size))
    pw.writeByte(my.info.lang)   // Charset number
    pw.writeZeros(23)            // Filler
    pw.writeNTB([]byte(my.user)) // Username
    pw.writeBin(scrPasswd)       // Encrypted password
    if len(my.dbname) > 0 {
        pw.writeNTB([]byte(my.dbname))
    }
    if len(my.dbname) > 0 {
        pay_len += len(my.dbname) + 1
        flags |= _CLIENT_CONNECT_WITH_DB
    }
    return
}

func (my *MySQL) oldPasswd() {
    /*
    if my.Debug {
        log.Printf("[%2d <-] Password packet", my.seq)
    }
    scrPasswd := encryptedOldPassword(my.passwd, my.info.scramble[:])
    pw := my.newPktWriter(len(scrPasswd) + 1)
    pw.write(scrPasswd)
    pw.writeByte(0)
    */
}