//go:generate bash ./g_version.sh
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/syslog"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	appName        = path.Base(os.Args[0])
	app            = kingpin.New(appName, "A telegraf input plugin that gatters metrics for every keyspace and table, by CrossEngage")
	checkName      = app.Flag("name", "Check name").Default(appName).String()
	jolokiaBaseURL = app.Flag("jolokia", "The base URL of the jolokia agent running on Cassandra JVM").Default("http://localhost:1778/jolokia").URL()
	debug          = app.Flag("debug", "If set, enables debug logs").Default("false").Bool()
	stderr         = app.Flag("stderr", "If set, enables logging to stderr instead of syslog").Default("false").Bool()
	skipZeros      = app.Flag("skip-zeros", "If set, it will not output metrics that only has zeros").Default("false").Bool()
	skipMetrics    = app.Flag("skip", "CSV with metric names to skip collection").Default(
		"CasCommitLatency",
		"CasCommitTotalLatency",
		"CasPrepareLatency",
		"CasPrepareTotalLatency",
		"CasProposeLatency",
		"CasProposeTotalLatency",
		"ColUpdateTimeDeltaHistogram",
		"CompressionMetadataOffHeapMemoryUsed",
		"CompressionRatio",
		"RowCacheHit",
		"RowCacheHitOutOfRange",
		"RowCacheMiss",
		"SpeculativeRetries",
	).Strings()
)

func main() {
	app.Version(version)
	kingpin.MustParse(app.Parse(os.Args[1:]))

	if *debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
	}

	if *stderr {
		log.SetOutput(os.Stderr)
	} else {
		slog, err := syslog.New(syslog.LOG_NOTICE|syslog.LOG_DAEMON, appName)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(slog)

	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	keys := []string{*checkName, "host=" + hostname}

	loc, err := url.Parse((*jolokiaBaseURL).String() + "/read/org.apache.cassandra.metrics:type=ColumnFamily,keyspace=*,scope=*,name=*")
	if err != nil {
		log.Fatal(err)
	}

	// TODO timeouts
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	resp, err := client.Get(loc.String())
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("%s %s", loc, resp.Status)
	}

	jsonResp := &jsonResp{}
	if err := json.NewDecoder(resp.Body).Decode(jsonResp); err != nil {
		log.Fatal(err)
	}

	if jsonResp.Status != 200 || jsonResp.Error != nil {
		log.Fatal(jsonResp.Error)
	}

	timestamp := time.Unix(jsonResp.TimeStamp, 0)
	commonKey := strings.Join(keys, ",")

	for keyPath, valueMap := range jsonResp.Value {
		keyPath = strings.Replace(keyPath, "org.apache.cassandra.metrics:", "", 1)
		if skipMetric(keyPath) {
			continue
		}

		keyParts := strings.Split(keyPath, ",")
		tags := []string{}
		for _, part := range keyParts {
			kv := strings.Split(part, "=")
			switch kv[0] {
			case "keyspace":
				tags = append(tags, "keyspace="+kv[1])
			case "name":
				tags = append(tags, "metric="+kv[1])
			case "scope":
				tags = append(tags, "cf="+kv[1])
			}
		}

		values := []string{}
		zeroValuesCount := 0
		numericValues := 0
		for valueKey, value := range valueMap {
			if value == nil {
				continue
			}
			rt := reflect.TypeOf(value)
			if rt.Kind() == reflect.Slice {
				continue
			}
			switch v := value.(type) {
			case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint:
				values = append(values, fmt.Sprintf(`%s=%di`, valueKey, v))
				numericValues++
				if v == 0 {
					zeroValuesCount++
				}
			case float32, float64, complex64, complex128:
				values = append(values, fmt.Sprintf(`%s=%f`, valueKey, v))
				numericValues++
				if v == 0.0 {
					zeroValuesCount++
				}
			case string:
				values = append(values, fmt.Sprintf(`%s="%s"`, valueKey, v))
			}
		}

		if *skipZeros && (zeroValuesCount == numericValues) {
			if *debug {
				log.Printf("Skipping `%s` because it has %d zero values of %d numeric values",
					keyPath, zeroValuesCount, numericValues)
			}
			continue
		}

		if len(values) > 0 {
			fmt.Print(commonKey, ",", strings.Join(tags, ","))
			fmt.Print(" ")
			fmt.Print(strings.Join(values, ","))
			fmt.Print(" ")
			fmt.Println(timestamp.UnixNano())
		}
	}
}

type jsonResp struct {
	Request struct {
		MBean string `json:"mbean"`
		Type  string `json:"type"`
	} `json:"request"`
	Status     int                               `json:"status"`
	Error      error                             `json:"error"`
	ErrorType  string                            `json:"error_type"`
	StackTrace string                            `json:"stacktrace"`
	TimeStamp  int64                             `json:"timestamp"`
	Value      map[string]map[string]interface{} `json:"value"`
}

func skipMetric(keyPath string) bool {
	for _, metricToSkip := range *skipMetrics {
		part := ",name=" + metricToSkip + ","
		if strings.Contains(keyPath, part) {
			if *debug {
				log.Printf("Skipping `%s` because it matches `%s`", keyPath, part)
			}
			return true
		}
	}
	return false
}
