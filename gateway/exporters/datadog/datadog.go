package datadog

import (
	"math"
	"strconv"
	"strings"

	dogstats "github.com/DataDog/datadog-go/v5/statsd"
	"github.com/openconfig/gnmi-gateway/gateway/configuration"
	"github.com/openconfig/gnmi-gateway/gateway/exporters"
	"github.com/openconfig/gnmi-gateway/gateway/utils"
	"github.com/openconfig/gnmi/cache"
	"github.com/openconfig/gnmi/ctree"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

const Name = "datadog"

var _ exporters.Exporter = new(DatadogExporter)

func init() {
	exporters.Register(Name, NewDatadogExporter)
}

func NewDatadogExporter(config *configuration.GatewayConfig) exporters.Exporter {

	return &DatadogExporter{
		config: config,
	}
}

type DatadogExporter struct {
	cache  *cache.Cache
	config *configuration.GatewayConfig
	client *dogstats.Client
}

func (e *DatadogExporter) Name() string {
	return Name
}

func (e *DatadogExporter) Export(leaf *ctree.Leaf) {
	notification := leaf.Value().(*gnmipb.Notification)
	for _, update := range notification.Update {

		value, isNumber := utils.GetNumberValues(update.Val)

		if !isNumber {
			if update.Val.GetDecimalVal() == nil {
				jsonValue, isJson := strconv.ParseFloat(string(update.Val.GetJsonVal()), 64)
				if isJson != nil {
					continue
				} else {
					value = jsonValue
				}
			} else {
				value = Round(update.Val)
			}
		}
		metric, tags := UpdateToMetricNameAndTags(notification.GetPrefix(), update)
		e.client.Gauge(e.config.Exporters.DatadogPrefix+metric, float64(value), tags, 1)
	}
}

func (e *DatadogExporter) Start(cache *cache.Cache) error {
	e.cache = cache
	e.config.Log.Info().Msg("Starting Datadog exporter.")

	e.client, _ = dogstats.New(
		e.config.Exporters.DatadogTarget,
	)

	return nil
}

func UpdateToMetricNameAndTags(prefix *gnmipb.Path, update *gnmipb.Update) (string, []string) {
	metricName := ""
	tags := make([]string, 0)

	if prefix != nil {
		target := prefix.GetTarget()
		if target != "" {
			tags = append(tags, "host:"+strings.Split(target, "_")[0])
		}
	}
	for _, prefixElem := range prefix.Elem {
		if metricName == "" {
			metricName = prefixElem.Name
		} else {
			metricName = metricName + "." + prefixElem.Name
		}
		for key, value := range prefixElem.Key {
			tagKey := prefixElem.Name + "_" + strings.ReplaceAll(key, "-", ".")
			value = strings.ReplaceAll(value, " ", "_")
			tags = append(tags, tagKey+":"+value)
		}
	}
	for _, elem := range update.Path.Elem {
		if metricName == "" {
			metricName = elem.Name
		} else {
			metricName = metricName + "." + elem.Name
		}
		for key, value := range elem.Key {
			tagKey := elem.Name + "_" + strings.ReplaceAll(key, "-", ".")
			value = strings.ReplaceAll(value, " ", "_")
			tags = append(tags, tagKey+":"+value)
		}
	}

	return strings.ReplaceAll(metricName, "-", "_"), tags
}

func Round(dec *gnmipb.TypedValue) float64 {
	return (float64(dec.GetDecimalVal().GetDigits()) / math.Pow(10, float64(dec.GetDecimalVal().Precision)))
}
