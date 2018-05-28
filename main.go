package main

import (
	"log"
	"os"
	"runtime"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/visheyra/nats-es-consumer/es"
	"go.uber.org/zap"
)

func main() { // nolint: gocyclo

	logger, err := zap.NewProduction()
	if err != nil {
		return
	}

	urls := os.Getenv("MY_NATSBOOTSTRAP")
	showTime := os.Getenv("MY_TIMESTAMP")
	subj := os.Getenv("MY_TOPIC")
	queue := os.Getenv("MY_QUEUE")
	esUrl := os.Getenv("MY_ELASTICSEARCH_ENDPOINT")
	esType := os.Getenv("MY_ELASTICSEARCH_TYPE")
	//nc, err = nats.Connect("tls://localhost:4443", nats.RootCAs("./configs/certs/ca.pem"))

	h, err := es.NewHandler(esUrl, esType)
	if err != nil {
		logger.Error("Error parsing ES url",
			zap.String("url", esUrl),
			zap.String("error", err.Error()),
		)
	}

	err = h.TestEndpoint()
	if err != nil {
		logger.Error("Endpoint is not healthy",
			zap.String("url", esUrl),
			zap.String("error", err.Error()),
		)
	}

	nc, err := nats.Connect(urls)
	if err != nil {
		logger.Error("Error nats connection:",
			zap.Error(err),
			zap.String("status", "ERROR"),
			zap.Duration("backoff", time.Second),
		)
		return
	}
	// defer nc.Close() // nolint: errcheck
	logger.Info("Connected",
		zap.String("target", urls),
		zap.String("ServerID", nc.ConnectedServerId()),
		zap.String("ConnectedServer", nc.ConnectedUrl()),
		zap.Strings("DiscoveredServers", nc.Servers()),
	)

	logger.Info("Dump Environment",
		zap.String("queue", queue),
		zap.String("urls", urls),
		zap.String("subj", subj),
		zap.String("showTime", showTime),
	)

	if queue != "" {
		_, err = nc.QueueSubscribe(subj, queue, h.Handle)
		if err != nil {
			logger.Error("Error nats subscription:",
				zap.Error(err),
				zap.String("status", "ERROR"),
			)
		}
		logger.Info("Subscription:",
			zap.String("topic", subj),
			zap.String("queue", queue),
			zap.Duration("backoff", time.Second),
		)
		err = nc.Flush()
		if err != nil {
			logger.Error("Error nats flush:",
				zap.Error(err),
				zap.String("status", "ERROR"),
				zap.Duration("backoff", time.Second),
			)
		}

		if err = nc.LastError(); err != nil {
			logger.Error("Error nats:",
				zap.Error(err),
				zap.String("status", "ERROR"),
				zap.Duration("backoff", time.Second),
			)
		}
		if showTime != "false" {
			log.SetFlags(log.LstdFlags)
		}

	} else {

		_, err = nc.Subscribe(subj, h.Handle)
		if err != nil {
			logger.Error("Error nats subscription:",
				zap.Error(err),
				zap.String("status", "ERROR"),
				zap.Duration("backoff", time.Second),
			)
		}
		err = nc.Flush()
		if err != nil {
			logger.Error("Error nats flush:",
				zap.Error(err),
				zap.String("status", "ERROR"),
				zap.Duration("backoff", time.Second),
			)
		}

		if err := nc.LastError(); err != nil {
			logger.Error("Error nats:",
				zap.Error(err),
				zap.String("status", "ERROR"),
				zap.Duration("backoff", time.Second),
			)
		}

		log.Printf("Listening on subject [%s]\n", subj)
		if showTime != "false" {
			log.SetFlags(log.LstdFlags)
		}

	}

	runtime.Goexit()
}
