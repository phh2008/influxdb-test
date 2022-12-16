package main

import (
	"context"
	"fmt"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"log"
	"math/rand"
	"testing"
	"time"
)
import influxdb2 "github.com/influxdata/influxdb-client-go/v2"

var token = "ChMZ8wRve6cyaUraOnmTIrPrEdT6yw_w2wD-ogKZkMtXSiJ-3NejHLwuiVZW5DYy0MP56RyChHJy1AJQXcxk-w=="
var url = "http://localhost:8086/"
var client = influxdb2.NewClient(url, token)
var org = "db01"
var bucket = "device"

func Test01(t *testing.T) {
	writeAPI := client.WriteAPIBlocking(org, bucket)
	for value := 0; value < 5; value++ {
		tags := map[string]string{
			"tagname1": "tagvalue1",
		}
		fields := map[string]interface{}{
			"field1": value,
		}
		point := write.NewPoint("measurement1", tags, fields, time.Now())
		time.Sleep(1 * time.Second) // separate points by 1 second
		if err := writeAPI.WritePoint(context.Background(), point); err != nil {
			log.Fatal(err)
		}
	}
}

func TestInfluxdb01(t *testing.T) {
	var writeAPI = client.WriteAPIBlocking(org, bucket)
	date := time.Now().Add(time.Hour * -1) //time.Parse("2006-01-02 15:04:05", "2022-09-01 00:00:00")
	rand.Seed(time.Now().UnixNano())
	names := []string{"tom", "jack", "lili", "lucy", "张三丰", "李四", "王五"}
	namesSize := len(names)
	local := []string{"坪山", "福田"}
	types := []string{"nas", "render"}
	for i := 1; i < 500000; i++ {
		dt := date.Add(time.Second * time.Duration(i))
		n := rand.Float64()*(39-35) + 35
		tp := fmt.Sprintf("%.1f", n)
		status := false
		if rand.Intn(2) == 1 {
			status = true
		}
		name := names[rand.Intn(namesSize)]

		tags := map[string]string{
			"local": local[rand.Intn(2)],
			"type":  types[rand.Intn(2)],
		}
		fields := map[string]interface{}{
			"name":        name,
			"status":      status,
			"temperature": tp,
		}
		fmt.Println("i=", i, "time: ", dt, " status: ", status, " temperature: ", tp, " name: ", name)
		point := write.NewPoint("test3", tags, fields, dt)
		if err := writeAPI.WritePoint(context.Background(), point); err != nil {
			log.Fatal("写入失败", err)
		}
	}
}

func TestInfluxdb02(t *testing.T) {
	queryAPI := client.QueryAPI(org)
	query := `from(bucket: "device")
				|> range(start: 0, stop: 2023-12-16T10:21:28Z)
				|> filter(fn: (r) => r._measurement == "test3" and r.local == "坪山" and r.type == "render")
				|> sort(columns: ["_time"], desc: true)
				//|> last()
				|> limit(n: 1, offset: 0)
				|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")`
	results, err := queryAPI.Query(context.Background(), query)
	if err != nil {
		log.Fatal(err)
	}
	for results.Next() {
		fmt.Println(results.Record())
	}
	if err := results.Err(); err != nil {
		log.Fatal(err)
	}
}
