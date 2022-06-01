/*******
* @Author:qingmeng
* @Description:
* @File:produce
* @Date2022/5/31
 */

package main

import (

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

	msg := broker.Msg{Id: 1102, Topic: "topic-test",  MsgType: 2,  Payload: "Hello World"}
	n, err := conn.Write(broker.MsgToBytes(msg))
	if err != nil {
		fmt.Print("write failed, err:", err)
	}

	fmt.Print(n)
}

