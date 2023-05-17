package main

/*
Copyright (c) 2023 Yunpeng Deng
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"
	rpprof "runtime/pprof"

	"github.com/dypflying/go-qcow2lib/cmd/subcmd"
)

var (
	daemonExitCh chan os.Signal
)

func main() {

	pprofWait := setupPprof()

	if err := subcmd.NewCommand().Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if pprofWait {
		daemonExitCh = make(chan os.Signal)
		signal.Notify(daemonExitCh, os.Interrupt, os.Kill)
		daemonize(context.Background())
	}
}

func setupPprof() bool {
	var profType string
	if profType = os.Getenv("PPROF"); profType != "" {
		defer time.Sleep(time.Second)
		if strings.ToLower(profType) == "http" {
			go func() {
				http.ListenAndServe("localhost:8081", nil)
			}()
			fmt.Println("check pprof at: http://localhost:8081/debug/pprof")
			return true
		} else {
			var pprofFile = "/tmp/qcow2pprof.profile"
			f, err := os.Create(pprofFile)
			if err != nil {
				log.Fatal(err)
			}
			rpprof.StartCPUProfile(f)
			defer rpprof.StopCPUProfile()
			fmt.Printf("check pprof at: %s\n", pprofFile)
		}
	}
	return false
}

func daemonize(ctx context.Context) {

	exitCh := ctx.Done()
	for {
		select {
		case <-daemonExitCh:
			fmt.Println("Catch exit signal, exiting")
			return
		case <-exitCh:
			fmt.Println("Context terminated,  exiting")
			return
		}
	}
}
