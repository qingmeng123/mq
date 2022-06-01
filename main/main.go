/*******
* @Author:qingmeng
* @Description:
* @File:main
* @Date2022/5/31
 */

package main

import (
	"fmt"
	"messageQueue/broker"
	"net"
)

func main()  {
	listen, err := net.Listen("tcp", "127.0.0.1:12345")
	if err != nil {
		fmt.Print("listen failed, err:", err)
		return
	}
	go broker.Save()
	for {
		conn, err := listen.Accept()
		if err != nil {
			fmt.Print("accept failed, err:", err)
			continue
		}
		go broker.Process(conn)

	}
}


