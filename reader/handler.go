package reader

import (
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/prometheus/prompb"
	"github.com/yuriadams/prometheus-elasticsearch-adapter/config"
)

// Handle receives the payload from Elasticsearch, format and send to Prometheus
func Handle(w http.ResponseWriter, r *http.Request) {
	timer := prometheus.NewTimer(config.ReadDuration)
	defer timer.ObserveDuration()

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.With("err", err).Error("Failed to read body.")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.With("err", err).Error("Failed to decompress body.")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	prometheus.NewTimer(config.ReadDuration)

	var req prompb.ReadRequest
	if err1 := proto.Unmarshal(reqBuf, &req); err1 != nil {
		log.With("err", err).Error("Failed to unmarshal body.")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Queries) != 1 {
		log.Error("More than one query sent.")
		http.Error(w, "Can only handle one query.", http.StatusBadRequest)
		return
	}

	_, reader := config.BuildClient()

	datapoints, err := reader.Read(&req)
	if err != nil {
		log.With("err", err).Error("Failed to run select.")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: responseToTimeseries(datapoints)},
		},
	}

	log.Infof("Returned %d time series.", len(resp.Results[0].Timeseries))
	data, err := proto.Marshal(&resp)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed = snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// responseToTimeseries parses structure from elasticsearch to prometheus strutcs
func responseToTimeseries(dataPoints []map[string]interface{}) []*prompb.TimeSeries {
	labelsToSeries := map[string]*prompb.TimeSeries{}

	for _, datapoint := range dataPoints {
		key := buildKey(datapoint)
		ts, ok := labelsToSeries[key]

		if !ok {
			labelPairs := make([]prompb.Label, 0, len(dataPoints)+1)

			for k, v := range datapoint {
				if k != "value" && k != "timestamp" {
					labelPairs = append(labelPairs, prompb.Label{
						Name:  k,
						Value: v.(string),
					})
				}
			}

			ts = &prompb.TimeSeries{
				Labels:  labelPairs,
				Samples: make([]prompb.Sample, 0, 100),
			}

			labelsToSeries[key] = ts
		}

		t, _ := time.Parse(time.RFC3339, datapoint["timestamp"].(string))
		timeInMillis := (t.UTC().UnixNano() / int64(time.Millisecond))

		ts.Samples = append(ts.Samples, prompb.Sample{
			Timestamp: timeInMillis,
			Value:     datapoint["value"].(float64),
		})

	}

	resp := make([]*prompb.TimeSeries, 0, len(labelsToSeries))

	for _, ts := range labelsToSeries {
		resp = append(resp, ts)
	}

	return resp
}

// each metric has a unique key with its labels to avoid duplication on the graph
func buildKey(datapoint map[string]interface{}) string {
	keys := make([]string, 0, len(datapoint))
	for k, v := range datapoint {
		if k != "value" && k != "timestamp" {
			keys = append(keys, v.(string))
		}
	}
	sort.Strings(keys)
	return strings.Join(keys[:], ",")
}
