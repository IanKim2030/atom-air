package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"
)

// OTAServer serves the firmware directory over plain HTTP so an Atom Lite can
// pull a .bin across the store LAN in about five seconds.
type OTAServer struct {
	Dir  string
	Port int
	srv  *http.Server
}

func NewOTAServer(dir string, port int) *OTAServer {
	return &OTAServer{Dir: dir, Port: port}
}

// Start binds the listener and serves in the background. A bind failure is
// reported but not fatal: sensor ingest and statistics must keep working even
// if something else already holds the OTA port.
func (o *OTAServer) Start() error {
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/", loggingFileServer(http.Dir(o.Dir)))

	o.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", net.JoinHostPort("0.0.0.0", itoa(o.Port)))
	if err != nil {
		return err
	}
	go func() {
		if err := o.srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			slog.Error("OTA HTTP server stopped", "err", err)
		}
	}()
	slog.Info("OTA HTTP server started", "dir", o.Dir, "port", o.Port)
	return nil
}

func (o *OTAServer) Stop() {
	if o.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = o.srv.Shutdown(ctx)
}

func loggingFileServer(root http.FileSystem) http.Handler {
	fs := http.FileServer(root)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.Debug("OTA fetch", "path", r.URL.Path, "from", r.RemoteAddr)
		fs.ServeHTTP(w, r)
	})
}
