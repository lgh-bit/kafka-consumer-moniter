package kafka_moniter

import (
	"container/list"
	"github.com/prometheus/client_golang/prometheus"
)


type ParamFunc func(string, string,string, string)(*list.List, *list.List)

type ConsumerInfoManager struct {
	Zone                    string  //唯一标识
	PartitionOffsetsDesc    *prometheus.Desc
	DelayLagDesc            *prometheus.Desc
	Params                  []string
}

func (c *ConsumerInfoManager) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.PartitionOffsetsDesc
	ch <- c.DelayLagDesc
}

func (c *ConsumerInfoManager) Collect(ch chan<- prometheus.Metric) {
	paramLen := len(c.Params)
	if paramLen != 4 {
		panic("param error")
	}
	Info.Printf("params: %s", c.Params)
	params := c.Params
	groupInfoList, parationList := GauageCollector(params[0], params[1], params[2], params[3])
	if groupInfoList == nil {
		return
	}
	aa := []string{"PartitionOffsetsDesc","PartitionLatestOffsetsDesc"}
	for paration := parationList.Front(); paration != nil; paration = paration.Next() {
		pa := paration.Value.(PartitionInfo)
		ch <- prometheus.MustNewConstMetric(
				c.PartitionOffsetsDesc,
				prometheus.CounterValue,
				float64(pa.PartitionLatestOffset),
				aa[1],
				pa.ClusterName,
				pa.Topic,
				pa.GroupName,
				pa.ConsumerId,
			)
		ch <- prometheus.MustNewConstMetric(
			c.PartitionOffsetsDesc,
			prometheus.CounterValue,
			float64(pa.PartitionOffset),
			aa[0],
			pa.ClusterName,
			pa.Topic,
			pa.GroupName,
			pa.ConsumerId,
		)
	}

	bb := []string{"PercentageCovered", "TotalLag"}
    for group := groupInfoList.Front(); group != nil; group = group.Next() {
    	gr := group.Value.(GroupInfo)
		ch <- prometheus.MustNewConstMetric(
			c.DelayLagDesc,
			prometheus.GaugeValue,
			float64(gr.PercentageCovered),
			bb[0],
			gr.ClusterName,
			gr.Topic,
			gr.GroupName,
		)

		ch <- prometheus.MustNewConstMetric(
			c.DelayLagDesc,
			prometheus.GaugeValue,
			float64(gr.TotalLag),
			bb[1],
			gr.ClusterName,
			gr.Topic,
			gr.GroupName,
		)
	}
}


func NewConsumerInfoManager(zone string, paramsData []string) *ConsumerInfoManager {
	return &ConsumerInfoManager{
		Zone: zone,
		PartitionOffsetsDesc: prometheus.NewDesc(
			"kafka_partition_offset",
			"kafka consumer one topic partition offset value.",
			[]string{"offset", "cluster", "topic", "groupName", "consumer"},
			prometheus.Labels{"zone": zone},
		),
		DelayLagDesc: prometheus.NewDesc(
			"kafka_consumer_monitor",
			"kafka consumer topic monitor value.",
			[]string{"key", "cluster", "topic", "groupName"},
			prometheus.Labels{"zone": zone},
		),
		Params: paramsData,
	}
}

