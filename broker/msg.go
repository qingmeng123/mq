/*******
* @Author:qingmeng
* @Description:
* @File:msg
* @Date2022/5/31
 */

package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Msg struct {
	Id int64
	TopicLen int64
	Topic string
	MsgType int64	//消息类型.1.consumer 2.producer 3.comsumer-ack 4.error
	Len int64  		//消息长度
	Payload string	//消息
}

func BytesToMsg(reader io.Reader) Msg {
	m:= Msg{}
	var buf [128]byte
	n,err:=reader.Read(buf[:])
	if err!=nil{
		fmt.Println("read failed,err:",err)
	}
	fmt.Println("read bytes:",n)

	//id
	buff:=bytes.NewBuffer(buf[0:8])
	binary.Read(buff,binary.LittleEndian,&m.Id)
	//topicLen
	buff=bytes.NewBuffer(buf[8:16])
	binary.Read(buff,binary.LittleEndian, &m.TopicLen)
	//topic
	msgLastIndex:=16+m.TopicLen
	m.Topic=string(buf[16:msgLastIndex])
	//msgType
	buff=bytes.NewBuffer(buf[msgLastIndex:msgLastIndex+8])
	binary.Read(buff,binary.LittleEndian,&m.MsgType)
	//msgLen
	buff=bytes.NewBuffer(buf[msgLastIndex+8:msgLastIndex+16])
	binary.Read(buff,binary.LittleEndian,&m.Len)
	if m.Len<=0{
		return m
	}
	m.Payload=string(buf[msgLastIndex+16:msgLastIndex+16+m.Len])
	return m
}

func MsgToBytes(msg Msg) []byte {
	msg.TopicLen=int64(len([]byte(msg.Topic)))
	msg.Len=int64(len([]byte(msg.Payload)))

	var data []byte
	buf := bytes.NewBuffer([]byte{})
	//写入缓存
	binary.Write(buf, binary.LittleEndian, msg.Id)

	binary.Write(buf, binary.LittleEndian, msg.TopicLen)
	data = append(data, buf.Bytes()...)

	data = append(data, []byte(msg.Topic)...)

	buf = bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.LittleEndian, msg.MsgType)

	binary.Write(buf, binary.LittleEndian, msg.Len)
	data = append(data, buf.Bytes()...)

	data = append(data, []byte(msg.Payload)...)

	return data


}