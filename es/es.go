package es

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/go-nats"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

//Handler struct for es forwarding
type Handler struct {
	url    string
	esType string
}

//NewHandler creates a new handler
func NewHandler(URL, esType string) (*Handler, error) {
	_, err := url.Parse(URL)
	if err != nil {
		return nil, err
	}

	return &Handler{
		url:    URL,
		esType: esType,
	}, nil
}

//Handle http handler
func (h Handler) Handle(m *nats.Msg) {

	logger, err := zap.NewProduction()
	if err != nil {
		logger.Error("Failed to create zap logger",
			zap.String("status", "ERROR"),
			zap.Duration("backoff", time.Second),
			zap.Error(err),
		)
		return
	}

	data := make(map[string]interface{})

	err = json.Unmarshal(m.Data, &data)
	if err != nil {
		logger.Error("Failed to load json from nats",
			zap.String("error", err.Error()),
		)
		return
	}

	data["time"] = time.Now().Format(time.RFC3339)
	repr, err := json.Marshal(data)
	if err != nil {
		logger.Error("Can't reserialize log")
	}

	index := strings.Replace(m.Subject, ".", "-", -1)
	err = h.postAtIndexWithDate(repr, index)
	if err != nil {
		logger.Error("Failed to post data at index",
			zap.String("error", err.Error()),
			zap.String("data", string(m.Data[:])),
		)

	} else {
		logger.Debug("Emitted sample")
	}
}

//TestEndpoint test that the elastic search endpoint is reachable and sane
func (h Handler) TestEndpoint() error {
	r, err := http.Get(h.url)

	if err != nil {
		return err
	}

	if r.StatusCode != 200 {
		return errors.New("Endpoint not replying typical 200 answer on ping")
	}

	return nil
}

func (h Handler) postAtIndexWithDate(data []byte, index string) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return err
	}

	finalIndex := strings.Join(
		[]string{index, time.Now().Format("2006-01-02")}, "-",
	)

	u, _ := url.Parse(h.url)
	u.Path = path.Join(finalIndex, h.esType)

	r, err := http.Post(u.String(), "application/json", bytes.NewReader(data))

	if err != nil {
		return err
	}

	if r.StatusCode < 200 && r.StatusCode >= 300 {
		defer r.Body.Close() // nolint: errcheck
		b, _ := ioutil.ReadAll(r.Body)
		return fmt.Errorf("Can't post data to ES code: %d message: [%s]", r.StatusCode, string(b[:]))
	}

	logger.Debug(
		"Suceeded sending sample",
		zap.Int("length", len(data)),
	)

	return nil
}
