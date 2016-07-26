package mgd

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
	"strings"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

// Mgd is a mgd plugin
type Mgd struct {
	Servers []string
}

type ServerStatus struct {
	Name    string `json:"name"`
	StartAt int    `json:"start-at"`
}

type MgdStatus struct {
	Server        ServerStatus             `json:"server"`
	Downsteram    []map[string]interface{} `json:"downsteram"`
	Upstream      []map[string]interface{} `json:"upstream"`
	Inversestream []map[string]interface{} `json:"inversestream"`
	Frontstream   []map[string]interface{} `json:"frontstream"`
}

var sampleConfig = `
  ## An array of address to gather stats about. Specify an ip on hostname
  ## with optional port. ie localhost, 10.0.0.1:50000, etc.
  servers = ["localhost:50000"]
`

var defaultTimeout = 5 * time.Second

// SampleConfig returns sample configuration message
func (m *Mgd) SampleConfig() string {
	return sampleConfig
}

// Description returns description of Mgd plugin
func (m *Mgd) Description() string {
	return "Read metrics from one or many mgd servers"
}

// Gather reads stats from all configured servers accumulates stats
func (m *Mgd) Gather(acc telegraf.Accumulator) error {
	if len(m.Servers) == 0 {
		return m.gatherServer(":50000", acc)
	}

	for _, serverAddress := range m.Servers {
		if err := m.gatherServer(serverAddress, acc); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mgd) gatherServer(
	address string,
	acc telegraf.Accumulator,
) error {
	var err error
	_, _, err = net.SplitHostPort(address)
	if err != nil {
		address = address + ":50000"
	}
	resource := fmt.Sprintf("http://%s/", address)
	res, err := http.Get(resource)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	mgdStatus, err := parseResponse(res.Body)
	if err != nil {
		return err
	}
	// Add server address as a tag
	tags := map[string]string{"server": address}

	// Process values
	// fields := make(map[string]interface{})
	// acc.AddFields("mgd", fields, tags)

	for _, inversestream := range mgdStatus.Inversestream {
		if err := m.gatherInversestream(tags, inversestream, acc); err != nil {
			return err
		}
	}
	for _, upstream := range mgdStatus.Upstream {
		if err := m.gatherUpsteam(tags, upstream, acc); err != nil {
			return err
		}
	}
	for _, downsteram := range mgdStatus.Downsteram {
		if err := m.gatherDownsteram(tags, downsteram, acc); err != nil {
			return err
		}
	}
	for _, frontstream := range mgdStatus.Frontstream {
		if err := m.gatherFrontstream(tags, frontstream, acc); err != nil {
			return err
		}
	}

	return nil
}

func (m *Mgd) gatherInversestream(
	tags map[string]string,
	status map[string]interface{},
	acc telegraf.Accumulator,
) error {
	fields := map[string]interface{}{
		"ok":                status["ok"],
		"jobs":              status["jobs"],
		"one-minute":        int(status["one-minute"].(float64)),
		"five-minute":       int(status["five-minute"].(float64)),
		"fifteen-minute":    int(status["fifteen-minute"].(float64)),
		"sw":                int(status["fifteen-minute"].(float64)),
		"sw-one-minute":     int(status["sw-one-minute"].(float64)),
		"sw-five-minute":    int(status["sw-five-minute"].(float64)),
		"sw-fifteen-minute": int(status["sw-fifteen-minute"].(float64)),
	}
	accTags := map[string]string{}
	for k, v := range tags {
		accTags[k] = v
	}
	accTags["name"] = status["name"].(string)
	acc.AddFields("inversestream", fields, accTags)
	return nil
}

func (m *Mgd) gatherUpsteam(
	tags map[string]string,
	status map[string]interface{},
	acc telegraf.Accumulator,
) error {
	accTags := map[string]string{}
	for k, v := range tags {
		accTags[k] = v
	}
	accTags["name"] = status["name"].(string)
	accTags["src"] = status["src"].(string)
	fields := map[string]interface{}{
		"ok":             status["ok"],
		"jobs":           status["jobs"],
		"fail":           status["fail"],
		"idling":         status["idling"],
		"success":        status["success"],
		"current":        status["current"],
		"one-minute":     int(status["one-minute"].(float64)),
		"five-minute":    int(status["five-minute"].(float64)),
		"fifteen-minute": int(status["fifteen-minute"].(float64)),
		"tr-min":         status["tr-min"],
		"tr-max":         status["tr-max"],
	}
	percentiles := status["tr-percentiles"].(map[string]interface{})
	fields["tr-50"] = percentiles["50"]
	fields["tr-75"] = percentiles["75"]
	fields["tr-95"] = percentiles["95"]
	fields["tr-99"] = percentiles["99"]
	fields["tr-999"] = percentiles["999"]

	acc.AddFields("upstream", fields, accTags)
	return nil
}

func (m *Mgd) gatherFrontstream(
	tags map[string]string,
	status map[string]interface{},
	acc telegraf.Accumulator,
) error {
	accTags := map[string]string{}
	for k, v := range tags {
		accTags[k] = v
	}
	fields := map[string]interface{}{
		"ok":             status["ok"],
		"jobs":           status["jobs"],
		"fail":           status["fail"],
		"one-minute":     int(status["one-minute"].(float64)),
		"five-minute":    int(status["five-minute"].(float64)),
		"fifteen-minute": int(status["fifteen-minute"].(float64)),
	}
	accTags["name"] = status["name"].(string)
	acc.AddFields("frontstream", fields, accTags)
	return nil
}

func (m *Mgd) gatherDownsteram(
	tags map[string]string,
	status map[string]interface{},
	acc telegraf.Accumulator,
) error {
	accTags := map[string]string{}
	for k, v := range tags {
		accTags[k] = v
	}
	accTags["name"] = status["name"].(string)
	accTags["app"] = status["app"].(string)

	fields := map[string]interface{}{
		"ok":             status["ok"],
		"jobs":           status["jobs"],
		"fail":           status["fail"],
		"success":        status["success"],
		"current":        status["current"],
		"one-minute":     int(status["one-minute"].(float64)),
		"five-minute":    int(status["five-minute"].(float64)),
		"fifteen-minute": int(status["fifteen-minute"].(float64)),
		"tr-min":         status["tr-min"],
		"tr-max":         status["tr-max"],
	}
	percentiles := status["tr-percentiles"].(map[string]interface{})
	fields["tr-50"] = percentiles["50"]
	fields["tr-75"] = percentiles["75"]
	fields["tr-95"] = percentiles["95"]
	fields["tr-99"] = percentiles["99"]
	fields["tr-999"] = percentiles["999"]

	for tag, value := range status {
		if strings.HasPrefix(tag, "code-") {
			code := tag[5:]
			fields := value.(map[string]interface{})
			tags := map[string]string{}
			for k, v := range accTags {
				tags[k] = v
			}
			tags["code"] = code
			acc.AddFields("dsc", fields, tags)
		} else if strings.HasPrefix(tag, "fbs-") {
			fbs := tag[4:]
			fields := value.(map[string]interface{})
			tags := map[string]string{}
			for k, v := range accTags {
				tags[k] = v
			}
			tags["fbs"] = fbs
			acc.AddFields("fbs", fields, tags)
		}
	}
	acc.AddFields("downsteram", fields, accTags)
	return nil
}

func parseResponse(r io.Reader) (*MgdStatus, error) {
	mgdStatus := &MgdStatus{}
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, mgdStatus); err != nil {
		return nil, err
	}
	return mgdStatus, nil
}

func init() {
	inputs.Add("mgd", func() telegraf.Input {
		return &Mgd{}
	})
}
