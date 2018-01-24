package main

import (
	_ "encoding/json"
	_ "fmt"
	"net/http"
	_ "net/http/pprof"
	_ "net/url"
	_ "regexp"
	_ "strconv"
	_ "strings"
	_ "sync"
	_ "time"

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

func gauge(name string) prometheus.Gauge {
  return prometheus.NewGauge(prometheus.GaugeOpts{
    Namespace: namespace,
    Subsystem: "cloudera",
    Name:      name,
    Help:      "Health of the named system.",
  })
}

var (
  hdfs_blocks_with_corrupt_replicated = gauge("hdfs_blocks_with_corrupt_replicated")
  hdfs_canary_health                  = gauge("hdfs_canary_health")
  hdfs_data_nodes_healthy             = gauge("hdfs_data_nodes_healthy")
  hdfs_free_space_remaining           = gauge("hdfs_free_space_remaining")
  hdfs_ha_namenode_health             = gauge("hdfs_ha_namenode_health")
  hdfs_missing_blocks                 = gauge("hdfs_missing_blocks")
  hdfs_under_replicated_blocks        = gauge("hdfs_under_replicated_blocks")
  impala_assignment_localitydisabled  = gauge("impala_assignment_localitydisabled")
  impala_catalogserver_health         = gauge("impala_catalogserver_health")
  impala_impalads_healthy             = gauge("impala_impalads_healthy")
  impala_statestore_health            = gauge("impala_statestore_health")
  yarn_jobhistory_health              = gauge("yarn_jobhistory_health")
  yarn_node_managers_healthy          = gauge("yarn_node_managers_healthy")
  yarn_resourcemanagers_health        = gauge("yarn_resourcemanagers_health")
  hive_hivemetastores_healthy         = gauge("hive_hivemetastores_healthy")
  hive_hiveserver2s_healthy           = gauge("hive_hiveserver2s_healthy")
  zookeeper_canary_health             = gauge("zookeeper_canary_health")
  zookeeper_servers_healthy           = gauge("zookeeper_servers_healthy")
  hue_hue_servers_healthy             = gauge("hue_hue_servers_healthy")
  oozie_oozie_servers_healthy         = gauge("oozie_oozie_servers_healthy")
)

func init() {
	prometheus.MustRegister(version.NewCollector("cloudera_exporter"))
  prometheus.MustRegister(hdfs_blocks_with_corrupt_replicated)
  prometheus.MustRegister(hdfs_canary_health)
  prometheus.MustRegister(hdfs_data_nodes_healthy)
  prometheus.MustRegister(hdfs_free_space_remaining)
  prometheus.MustRegister(hdfs_ha_namenode_health)
  prometheus.MustRegister(hdfs_missing_blocks)
  prometheus.MustRegister(hdfs_under_replicated_blocks)
  prometheus.MustRegister(impala_assignment_localitydisabled)
  prometheus.MustRegister(impala_catalogserver_health)
  prometheus.MustRegister(impala_impalads_healthy)
  prometheus.MustRegister(impala_statestore_health)
  prometheus.MustRegister(yarn_jobhistory_health)
  prometheus.MustRegister(yarn_node_managers_healthy)
  prometheus.MustRegister(yarn_resourcemanagers_health)
  prometheus.MustRegister(hive_hivemetastores_healthy)
  prometheus.MustRegister(hive_hiveserver2s_healthy)
  prometheus.MustRegister(zookeeper_canary_health)
  prometheus.MustRegister(zookeeper_servers_healthy)
  prometheus.MustRegister(hue_hue_servers_healthy)
  prometheus.MustRegister(oozie_oozie_servers_healthy)
}

type ClouderaHealthCheck struct {
  name string
  summary string
}

type ClouderaItem struct {
  name string
  `type` string
  serviceUrl string
  serviceState string
  healthSummary string
  healthChecks []ClouderaHealthCheck
}

type ClouderaResponse struct {
  Items []ClouderaItem `json:items`
}

func getHealth(opts clouderaOpts) ClouderaResponse {
  req, err := http.NewRequest("GET", opts.uri, nil)
  req.SetBasicAuth(opts.username, opts.password)
  cli := &http.Client{}
  resp, err := cli.Do(req)
}




func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9107").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		// kvPrefix      = kingpin.Flag("kv.prefix", "Prefix from which to expose key/value pairs.").Default("").String()
		// kvFilter      = kingpin.Flag("kv.filter", "Regex that determines which keys to expose.").Default(".*").String()

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

  // exporter, err := NewExporter(opts, *kvPrefix, *kvFilter, false)
  // if err != nil {
  //   log.Fatalln(err)
  // }
  // prometheus.MustRegister(exporter)

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