package srt

import (
	"testing"
	"time"
)

func TestBufferOverflowClosesConnection(t *testing.T) {
	for _, direction := range []string{"receive", "send"} {
		t.Run(direction, func(t *testing.T) {
			config := DefaultConfig()
			config.ConnectionTimeout = time.Second
			if direction == "receive" {
				config.ReceiverBufferSize = 1024
			}
			listener, err := Listen("srt", "127.0.0.1:0", config)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			accepted := make(chan Conn, 1)
			failures := make(chan error, 1)
			go func() {
				request, err := listener.Accept2()
				if err != nil {
					failures <- err
					return
				}
				if err = request.SetPassphrase("test-passphrase-32-bytes-long!!!!!"); err != nil {
					request.Reject(REJ_BADSECRET)
					failures <- err
					return
				}
				conn, err := request.Accept()
				if err != nil {
					failures <- err
					return
				}
				accepted <- conn
			}()
			callerConfig := DefaultConfig()
			callerConfig.ConnectionTimeout = time.Second
			callerConfig.Passphrase = "test-passphrase-32-bytes-long!!!!!"
			if direction == "send" {
				callerConfig.SendBufferSize = 1024
			}
			caller, err := Dial("srt", listener.Addr().String(), callerConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer caller.Close()
			var server Conn
			select {
			case server = <-accepted:
			case err := <-failures:
				t.Fatal(err)
			case <-time.After(2 * time.Second):
				t.Fatal("accept timeout")
			}
			defer server.Close()
			target := server.(*srtConn)
			if direction == "send" {
				target = caller.(*dialer).conn
			}
			if _, err := caller.Write(make([]byte, 1316)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-target.ctx.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("transport buffer overflow did not close connection")
			}
		})
	}
}
