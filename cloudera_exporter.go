package main

import (
	"encoding/json"
	_ "fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

type clouderaOpts struct {
  uri string
  username string
  password string
  clusterName string
}

const (
  namespace = "cloudera"
)

func gaugeVec(name string) *prometheus.GaugeVec {
  return prometheus.NewGaugeVec(prometheus.GaugeOpts{
      Namespace: namespace,
      Subsystem: "services",
      Name:      name,
      Help:      "Health of the services.",
    },
    []string{"name"},
  )
}

var (
  servicesHealthGauge = gaugeVec("health")
)

func init() {
	prometheus.MustRegister(version.NewCollector("cloudera_exporter"))
  prometheus.MustRegister(servicesHealthGauge)
}

type ClouderaHealthCheck struct {
  Name string
  Summary string
}

type ClusterRef struct {
  ClusterName string
}

type ClouderaItem struct {
  Name string
  thetype string `json:"type"`
  ClusterRef ClusterRef
  ServiceUrl string
  ServiceState string
  HealthSummary string
  HealthChecks []ClouderaHealthCheck
  ConfigStale bool
}

type ClouderaResponse struct {
  Items []ClouderaItem 
}

func getMetrics(opts clouderaOpts) (ClouderaResponse, error) {
  path := "/api/v1/clusters/" + opts.clusterName + "/services/"
  clouderaResponse := &ClouderaResponse{}

  req, err := http.NewRequest("GET", opts.uri+path, nil)
  if err != nil {
    return *clouderaResponse, err
  }
  req.SetBasicAuth(opts.username, opts.password)

  client := &http.Client{}
  resp, err := client.Do(req)
  if err != nil {
    return *clouderaResponse, err
  }

  err = json.NewDecoder(resp.Body).Decode(&clouderaResponse)
  if err != nil {
    return *clouderaResponse, err
  }
  
  return *clouderaResponse, nil
}

func updateMetric(name string, healthSummary string) {
  status := 0.0
  if (healthSummary == "GOOD") { status = 1.0 }
  log.Debugln("Updating metric %s = %f", name, status)
  servicesHealthGauge.WithLabelValues(name).Set(status)
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9107").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()

    opts = clouderaOpts{}
	)

  kingpin.Flag("cloudera.uri", "Address and port of the cloudera api").Default(":7180").StringVar(&opts.uri)
  kingpin.Flag("cloudera.user", "Username").Default("admin").StringVar(&opts.username)
  kingpin.Flag("cloudera.password", "Password").Default("").StringVar(&opts.password)
  kingpin.Flag("cloudera.clustername", "apui path").Default("Cluster%201").StringVar(&opts.clusterName)

  log.AddFlags(kingpin.CommandLine)
  kingpin.Version(version.Print("cloudera_exporter"))
  kingpin.HelpFlag.Short('h')
  kingpin.Parse()

  log.Infoln("Starting cloudera_exporter", version.Info())
  log.Infoln("Build context", version.BuildContext())

  go func() {
    for {
      clouderaResponse, err := getMetrics(opts)
      if err != nil {
          log.Fatalln(err)
      }
      for _, item := range clouderaResponse.Items {
        updateMetric(item.Name, item.HealthSummary)
      } 
      time.Sleep(time.Duration(10000 * time.Millisecond))
    }
  }()

  http.Handle(*metricsPath, prometheus.Handler())
  http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
    w.Write([]byte(`<html>
             <head><title>Cloudera Exporter</title></head>
             <body>
             <h1>Cloudera Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             <h2>Build</h2>
             <pre>` + version.Info() + ` ` + version.BuildContext() + `</pre>
             </body>
             </html>`))
  })

  log.Infoln("Listening on", *listenAddress)
  log.Fatal(http.ListenAndServe(*listenAddress, nil))
}