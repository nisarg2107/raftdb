package main

import (
	"net/http"
	"net/http/pprof"
	"time"
)

func serveProfile(address string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return (&http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe()
}
