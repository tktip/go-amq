package swagex

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
)

// global swagger details
// @title Go-LDAP swagger
// @version 1.0
// @description Swagger for go-LDAP integration.
// @description Go-LDAP is a go version of ldap-bridge.

// @contact.name TIP

// @host localhost:7070
// @BasePath /

// @securityDefinitions.basic BasicAuth

var (
	swaggerDoc = []byte("{}")
)

func init() {
	swaggerLoc := os.Getenv("SWAGGER_LOCATION")
	if swaggerLoc == "" {
		swaggerLoc = "/swagger.json"
	}

	var err error
	swaggerDoc, err = ioutil.ReadFile(swaggerLoc)
	if err != nil || swaggerDoc == nil {
		log.Warn("Could not read swagger doc: " + err.Error())
		swaggerDoc = []byte("{}")
	}
}

//individual route details.
// @Summary Swagger doc.
// @Description Returns the swagger doc.
// @Produce application/json
// @Success 200 {string} string "The swagger json."
// @Success 404 {string} string "If swagger has not been added."
// @Router /api-doc [get]
func swaggerEndpoint(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("content-type", "application/json; charset=UTF-8")
	w.Write(swaggerDoc)
}

type exampleData struct {
	ExampleInt    int
	ExampleString string
}

//individual route details.
// @Summary dummy end point.
// @Description Just a dummy post endpoint.
// @Produce application/json
// @Success 200 {object} swagex.exampleData "If this is a post request, return some json!"
// @Success 400 {string} string "If the request wasn't post"
// @Success 500 {string} string "If marshalling of struct failed"
// @Router /getStruct [post]
func swaggerGetStruct(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("I only accept POST requests"))
		return
	}

	example := exampleData{
		0,
		"Some text",
	}

	data, err := json.Marshal(&example)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("couldn't marshal response!"))
		return
	}

	//return json data.
	w.Header().Set("content-type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

//SetUpSwagger sets up a sample swagger end point.
func SetUpSwagger() {
	http.HandleFunc("/api-doc", swaggerEndpoint)
	http.HandleFunc("/getStruct", swaggerGetStruct)

	//use default handler
	log.Fatal(http.ListenAndServe(":7070", nil))
}