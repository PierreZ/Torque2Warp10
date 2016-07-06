package main

import (
	"bytes"
	"encoding/csv"
	"log"
	"net/http"
	"os"
)

// GTS is a representation of a Geo Time Series
// Please see http://www.warp10.io/apis/gts-input-format/
type GTS struct {
	TS     string // Timestamp of the reading, in microseconds since the Unix Epoch
	Lat    string // geographic coordinates of the reading
	Long   string // geographic coordinates of the reading
	Elev   string // elevation of the reading, in millimeters
	Name   string // Class name
	Labels string // Comma separated list of labels, using the syntax `key=value`
	Value  string // The value of the reading
}

// Print respects the following format:
// TS/LAT:LON/ELEV NAME{LABELS} VALUE
func (gts GTS) Print() []byte {
	return []byte(gts.TS + "/" + gts.Lat + ":" + gts.Long + "/" + gts.Elev + " " + gts.Name + "{" + gts.Labels + "}" + " " + gts.Value)
}

// TorqueKey is the struct generated from the CSV
type TorqueKey struct {
	MetricName string
	Tag        string
}

// TorqueKeys is the slice of TorqueKey
type TorqueKeys map[string]TorqueKey

var (
	warp10Endpoint = os.Getenv("WARP10_ENDPOINT")
	warp10Token    = os.Getenv("WARP10_TOKEN")
	torqueKeys     TorqueKeys
)

func main() {
	http.HandleFunc("/", query)
	http.ListenAndServe(":8080", nil)
}

func query(w http.ResponseWriter, r *http.Request) {
	// kff1005 refers to longitude
	// kff1006 refers to latitude
	// kff1010 refers to altitude
	longitude := r.URL.Query().Get("kff1005")
	latitude := r.URL.Query().Get("kff1006")
	altitude := r.URL.Query().Get("kff1010")
	time := r.URL.Query().Get("time")

	query := r.URL.Query()
	log.Println(query)

	// Let's loop all the GET parameters!
	for key := range query {
		// We need to map the deviceID with metric Name
		if val, ok := torqueKeys[key]; ok {
			sendToWarp10(GTS{time, latitude, longitude, altitude, val.MetricName, val.Tag, r.URL.Query().Get(key)})
		}
	}
}

// sendToWarp10 is used to push a GTS to a Warp10 datastore
func sendToWarp10(gts GTS) {
	log.Println(gts.Print())
	req, err := http.NewRequest("POST", warp10Endpoint+"/api/v0/update", bytes.NewBuffer(gts.Print()))
	req.Header.Set("X-Warp10-Token", warp10Token)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
}

// init is used to load a map to bind Torque's keys to appropriate name for metrics
func init() {

	torqueKeys = make(map[string]TorqueKey)
	// Checking env var
	if len(warp10Endpoint) == 0 || len(warp10Token) == 0 {
		log.Println("You moron forget to put the tokens!")
		os.Exit(1)
	}

	// Get CSV
	resp, err := http.Get("https://raw.githubusercontent.com/PierreZ/Torque2Warp10/master/keys.csv")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	reader := csv.NewReader(resp.Body)

	reader.FieldsPerRecord = 3

	rawCSVdata, err := reader.ReadAll()

	if err != nil {
		log.Println("Error in parsing:")
		log.Println(err)
		os.Exit(1)
	}

	// Let's feed our slice !
	for key, each := range rawCSVdata {
		// Avoid first line
		if key != 0 {
			log.Printf("%s: %s - %s\n", each[0], each[1], each[2])
			torqueKeys[each[0]] = TorqueKey{each[1], each[2]}
		}
	}
}
