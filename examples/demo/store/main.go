// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "modernc.org/sqlite"
)

const readHeaderTimeout = 10 * time.Second

func main() {
	dbPath := "demo.db"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		log.Fatal(err)
	}
	// 4 balances serialisation (1) vs WAL lock contention (16+) within busy_timeout.
	db.SetMaxOpenConns(4)
	if _, err := db.Exec("CREATE TABLE IF NOT EXISTS events (ts INTEGER, data TEXT)"); err != nil {
		log.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	inserts := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "store_inserts_total",
		Help: "Total number of successful inserts into the store.",
	})
	failures := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "store_insert_failures_total",
		Help: "Total number of failed inserts into the store.",
	})
	duration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "store_insert_duration_seconds",
		Help:    "Latency of the insert path, including body read and sql exec.",
		Buckets: prometheus.DefBuckets,
	})
	reg.MustRegister(inserts, failures, duration)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			failures.Inc()
			duration.Observe(time.Since(start).Seconds())
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
		if _, err := db.Exec("INSERT INTO events (ts, data) VALUES (?, ?)", time.Now().Unix(), string(body)); err != nil {
			failures.Inc()
			duration.Observe(time.Since(start).Seconds())
			http.Error(w, "insert failed", http.StatusInternalServerError)
			return
		}
		inserts.Inc()
		duration.Observe(time.Since(start).Seconds())
		fmt.Fprint(w, "ok")
	})

	addr := ":8080"
	if v := os.Getenv("STORE_ADDR"); v != "" {
		addr = v
	}

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: readHeaderTimeout}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt)
		<-sigCh
		srv.Close() //nolint:errcheck
	}()

	log.Printf("store listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
