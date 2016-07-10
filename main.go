package main

import (
	"bytes"
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"strconv"
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
	log.Println(gts.TS + "/" + gts.Lat + ":" + gts.Long + "/" + gts.Elev + " " + gts.Name + "{" + gts.Labels + "}" + " " + gts.Value)
	return []byte(gts.TS + "/" + gts.Lat + ":" + gts.Long + "/" + gts.Elev + " " + gts.Name + "{" + gts.Labels + "}" + " " + gts.Value)
}

// TorqueKey is the struct generated from the CSV
type TorqueKey struct {
	MetricName string
	Tag        string
}

// TorqueKeys is the slice of TorqueKey
type TorqueKeys map[string]TorqueKey

// Users represents all the Torque users allowed.
// A Torque user is represent by an ID and a email
type Users []string

var (
	warp10Endpoint = os.Getenv("WARP10_ENDPOINT")
	warp10Token    = os.Getenv("WARP10_TOKEN")
	torqueKeys     TorqueKeys
	user           = os.Getenv("ALLOWED_USERS")
)

func main() {
	http.HandleFunc("/api/torque", query)
	http.ListenAndServe(":8080", nil)
}

func query(w http.ResponseWriter, r *http.Request) {

	log.Println("Received a call HTTP, GET Keys:")
	log.Println(r.URL.Query())
	log.Println("longitude:", r.URL.Query().Get("kff1005"))
	log.Println("latitude:", r.URL.Query().Get("kff1006"))
	log.Println("elevation:", r.URL.Query().Get("kff1010"))
	log.Println("user:", r.URL.Query().Get("eml"))
	// Data are useless if they're not geolocalized or user is not authorized
	if len(r.URL.Query().Get("kff1005")) == 0 || len(r.URL.Query().Get("kff1006")) == 0 || len(r.URL.Query().Get("kff1010")) == 0 || r.URL.Query().Get("eml") == user {
		log.Println("No GPS Data, moving on")
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte("OK!"))
		return
	}
	// log.Println("longitude:", r.URL.Query().Get("kff1005"), "latitude:", r.URL.Query().Get("kff1006"), "elevation:", r.URL.Query().Get("kff1010"))
	// kff1005 refers to longitude
	// kff1006 refers to latitude
	// kff1010 refers to altitude
	id := r.URL.Query().Get("id")
	longitude := r.URL.Query().Get("kff1005")
	latitude := r.URL.Query().Get("kff1006")
	i, err := strconv.ParseFloat(r.URL.Query().Get("kff1010"), 64)
	if err != nil {
		log.Println(err)
	}
	altitude := strconv.Itoa(int(i * 1000.0))
	time := r.URL.Query().Get("time")

	// query contains all the get parameters
	query := r.URL.Query()

	// Let's loop all the GET parameters!
	for key := range query {
		// We need to map the deviceID with metric Name
		if val, ok := torqueKeys[key]; ok {

			// ID is part of the tags
			if (len(val.Tag)) == 0 {
				val.Tag = "id=" + id
			} else {
				val.Tag = "id=" + id + "," + val.Tag
			}

			sendToWarp10(GTS{time + "000", latitude, longitude, altitude, val.MetricName, val.Tag, r.URL.Query().Get(key)})
		}
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte("OK!"))
}

// sendToWarp10 is used to push a GTS to a Warp10 datastore
func sendToWarp10(gts GTS) {
	req, err := http.NewRequest("POST", warp10Endpoint+"/api/v0/update", bytes.NewBuffer(gts.Print()))
	req.Header.Set("X-Warp10-Token", warp10Token)
	client := &http.Client{}
	resp, err := client.Do(req)
	if resp.StatusCode != 200 {
		log.Panicln("Error", resp.StatusCode, ":", resp.Body)
	}
	if err != nil {
		log.Println(err)
	}
	log.Println("HTTP Return code", resp.StatusCode, "-", gts.Name, ":", gts.Value)
	defer resp.Body.Close()
}

// init is used to load a map to bind Torque's keys to appropriate name for metrics
func init() {

	log.Println("New user", user)

	// Get Torque Keys
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
		log.Println("Error in parsing:", err)
		os.Exit(1)
	}

	// Let's feed our slice !
	for key, each := range rawCSVdata {
		// Avoid first line
		if key != 0 {
			// log.Printf("%s: %s - %s\n", each[0], each[1], each[2])
			torqueKeys[each[0]] = TorqueKey{each[1], each[2]}
		}
	}
}
