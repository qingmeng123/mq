/*******
* @Author:qingmeng
* @Description:
* @File:cous
* @Date2022/6/1
 */

package main

import (

	"bytes"
	"fmt"
	"messageQueue/broker"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:12345")
	if err != nil {
		fmt.Print("connect failed, err:", err)
	}
	defer conn.Close()

	msg := broker.Msg{Topic: "topic-test",  MsgType: 1}

	n, err := conn.Write(broker.MsgToBytes(msg))
	if err != nil {
		fmt.Println("write failed, err:", err)
	}
	fmt.Println("n", n)

	var res [128]byte
	conn.Read(res[:])
	buf := bytes.NewBuffer(res[:])
	receMsg := broker.BytesToMsg(buf)
	fmt.Print(receMsg)

	// ack

}
