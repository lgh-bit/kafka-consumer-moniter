package main

import (
	"bufio"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"io"
	"kafkaMoniter/kafka_moniter"
	"net/http"
	"os"
	"strings"
)

func main() {
	file, err := os.Open("config")
	if err != nil {
		panic(err)
	}
    defer file.Close()
	rd := bufio.NewReader(file)
	reg := prometheus.NewPedanticRegistry()
	for {
		line, err := rd.ReadString('\n') //以'\n'为结束符读入一行

		if err != nil || io.EOF == err {
			break
		}
		params := strings.Split(line, ",")
		fmt.Println(params)
		if len(params) != 5 {
			panic("params error")
		}
		workerHt := kafka_moniter.NewConsumerInfoManager(params[4], params[0:4])
		reg.MustRegister(workerHt)
	}


	gatherers := prometheus.Gatherers{
		prometheus.DefaultGatherer,
		reg,
	}

	h := promhttp.HandlerFor(gatherers,
		promhttp.HandlerOpts{
			ErrorLog:      kafka_moniter.Error,
			ErrorHandling: promhttp.ContinueOnError,
		})
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
	})
	kafka_moniter.Info.Println("Start server at :9209")
	if err := http.ListenAndServe(":9209", nil); err != nil {
		kafka_moniter.Error.Printf("Error occur when start server %v", err)
		os.Exit(1)
	}

}
