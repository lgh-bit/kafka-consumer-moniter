package kafka_moniter

import (
	"container/list"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

type ConsumerInfo struct {
	Name                   string
	TotalLag               int
	PercentageCovered      int
	PartitionOffsets       []int
	PartitionLatestOffsets []int
	Owners                 []string
}

type PartitionInfo struct {
	ClusterName                string
	GroupName                  string
    Topic                      string
	ConsumerId                 string
	PartitionOffset            int
	PartitionLatestOffset      int
}

type GroupInfo struct {
	ClusterName                string
	GroupName                  string
	Topic                      string
	PercentageCovered          int
	TotalLag                   int
}

func GauageCollector(cluster_name string, consumer_name string, host string, authentication string) (*list.List, *list.List){
	partitions := list.New()
	groupInfos := list.New()
	meteData := FetchMetaDate(cluster_name, consumer_name, host, authentication)
	if meteData == "" {
		Warning.Println("fetch metaDate error")
		return nil,nil
	} else {
		consumers := ParseMeteDate(meteData)
		for consumer := consumers.Front(); consumer != nil; consumer = consumer.Next() {
			consumerInfo := consumer.Value.(ConsumerInfo)
			Info.Printf("name: %s, TotalLag: %d, PercentageCovered:%d",
				consumerInfo.Name, consumerInfo.TotalLag, consumerInfo.PercentageCovered)
			partitionOffsets := consumerInfo.PartitionOffsets
			partitionLatestOffsets := consumerInfo.PartitionLatestOffsets
			owners := consumerInfo.Owners
			if  len(partitionOffsets) != 0 &&
				len(partitionLatestOffsets) == len(partitionOffsets) &&
				len(partitionOffsets) == len(owners) {
				length := len(partitionOffsets)
				for i := 0; i < length; i++ {
					partition := PartitionInfo{}
					partition.ClusterName = cluster_name
					partition.GroupName = consumer_name
					partition.PartitionOffset = partitionOffsets[i]
					partition.PartitionLatestOffset = partitionLatestOffsets[i]
					partition.Topic = consumerInfo.Name
					partition.ConsumerId = owners[i]
					partitions.PushFront(partition)
				}
                groupInfo := GroupInfo{}
                groupInfo.Topic = consumerInfo.Name
                groupInfo.PercentageCovered = consumerInfo.PercentageCovered
                groupInfo.TotalLag = consumerInfo.TotalLag
                groupInfo.ClusterName = cluster_name
                groupInfo.GroupName = consumer_name
                groupInfos.PushFront(groupInfo)
			}
		}

	}
	return groupInfos, partitions
}

func ParseMeteDate(jsonStr string) *list.List {
	consumers := list.New()
	var dat map[string]map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &dat); err == nil {
		for key, value := range dat {
			var consumerInfo ConsumerInfo
			consumerInfo.Name = key

			for k, v := range value {
				switch vv := v.(type) {
				case float64:
					if k == "totalLag" {
						consumerInfo.TotalLag = int(vv)
					} else if k == "percentageCovered" {
						consumerInfo.PercentageCovered = int(vv)
					}
				case []interface{}:
					if k == "partitionOffsets" {
						consumerInfo.PartitionOffsets = make([]int, len(vv))
						for i, j := range vv {
							consumerInfo.PartitionOffsets[i] = int(j.(float64))
						}
					} else if k == "partitionLatestOffsets" {
						consumerInfo.PartitionLatestOffsets = make([]int, len(vv))
						for i, j := range vv {
							consumerInfo.PartitionLatestOffsets[i] = int(j.(float64))
						}
					} else if k == "owners" {
						consumerInfo.Owners = make([]string, len(vv))
						for i, j := range vv {
							consumerInfo.Owners[i] = j.(string)
						}
					}
				}
			}
			consumers.PushFront(consumerInfo)
		}

	}
	return consumers
}
func FetchMetaDate(cluster_name string, consumer_name string, host string, authentication string) string {
	params := map[string]string{
		"cluster_name":                     cluster_name,                  //集群名称
		"consumer_name":                    consumer_name,                 //消费者名称
		"host":                             host,                          //kafka manager host:port
		"play-basic-authentication-filter": authentication,                //验证
	}

	targetUrl := fmt.Sprintf("http://%s/api/status/%s/%s/KF/groupSummary", params["host"],
		params["cluster_name"], params["consumer_name"])
	Url, err := url.Parse(targetUrl)
	if err != nil {
		Error.Printf("FetchMetaDate err")
		//panic(err)
		return ""
	}
	urlPath := Url.String()
	Info.Println("urlPath: {}", urlPath)
	client := &http.Client{}
	req, _ := http.NewRequest("GET", urlPath, nil)
	req.Header.Add("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9")
	req.Header.Add("Accept-Encoding", "gzip, deflate, br")
	req.Header.Add("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36")
	req.Header.Add("Cookie", params["play-basic-authentication-filter"])

	resp, err := client.Do(req)
	if err != nil {
		Error.Println("get error, url:{}", urlPath)
		//panic(err)
		return ""
	}
	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	Info.Println("resp body: {}", string(body))
	Info.Println(resp.StatusCode)
	if resp.StatusCode != 200 {
		fmt.Println("request error")
		Error.Println("request status error, url:{}, status", urlPath, resp.StatusCode)
	}
	return string(body)
}
