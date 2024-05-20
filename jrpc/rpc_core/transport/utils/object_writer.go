package transport_utils

import (
	"encoding/binary"
	"jdb/jrpc/rpc_common/entities"
	"jdb/jrpc/rpc_core/serializer"
	"net"
)

const (
	MagicNumber = 0xCAFEBABE
	headerLen   = 12 // MagicNumber + serializerCode + dataLen
)

type ObjectWriter struct {
	conn net.Conn
}

func NewObjectWriter(conn net.Conn) *ObjectWriter {
	return &ObjectWriter{conn}
}

func (ow *ObjectWriter) WriteObject(data *entities.RPCdata, serializer serializer.CommonSerializer) error {
	// fmt.Println("序列化前data: ", data)
	dataByte, err := serializer.Serialize(data)
	// fmt.Println("序列化后dataByte: ", dataByte)
	if err != nil {
		return err
	}
	// fmt.Println("headerLen+len(dataByte)为: ", headerLen+len(dataByte))
	buf := make([]byte, headerLen+len(dataByte))
	binary.BigEndian.PutUint32(buf[:headerLen/3], uint32(MagicNumber))
	binary.BigEndian.PutUint32(buf[headerLen/3:headerLen/3*2], uint32(serializer.GetCode()))
	binary.BigEndian.PutUint32(buf[headerLen/3*2:headerLen], uint32(len(dataByte)))
	copy(buf[headerLen:], dataByte)
	_, err = ow.conn.Write(buf)
	if err != nil {
		return err
	}
	return nil
}
