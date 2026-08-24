// Command atomair-gateway is the Atom Air store gateway: a single static
// binary that runs either in the foreground or as a Windows service.
//
//	Atom Lite <--MQTT (Mosquitto)--> atomair-gateway <--WebSocket--> Cloud
//
// Every subsystem runs as its own goroutine, so the on-demand live stream can
// be switched on and off without ever interrupting local persistence or the
// licence checks. See gateway.go for the loops.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// AppVersion is reported to the cloud and written to the log on startup.
const AppVersion = "1.0.0"

// Config is the fully resolved runtime configuration.
type Config struct {
	StoreID         string
	CloudWS         string
	CloudHTTP       string
	Token           string
	MQTTHost        string
	MQTTPort        int
	DataDir         string
	DBPath          string
	LicenseConfig   string
	FirmwareDir     string
	LogFile         string
	LogLevel        string
	LogMaxMB        int
	LogBackups      int
	GraceDays       int
	LicenseInterval time.Duration
	OTAHost         string
	OTAPort         int
	Simulate        bool
	NoMQTT          bool
	Devices         int
}

func defaultDataDir() string {
	// A Windows service runs as LocalSystem with its working directory set to
	// system32, and Program Files is not writable. ProgramData is the correct
	// home for service state.
	if dir := os.Getenv("ProgramData"); dir != "" {
		return filepath.Join(dir, "AtomAir")
	}
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// parseConfig builds the config from flags, then resolves every path so that
// nothing depends on the process working directory.
func parseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("atomair-gateway", flag.ContinueOnError)
	var c Config
	var licenseIntervalSec float64

	fs.StringVar(&c.StoreID, "store-id", envOr("ATOM_STORE_ID", "S001"), "store identifier")
	fs.StringVar(&c.CloudWS, "cloud-ws", envOr("ATOM_CLOUD_WS", "ws://127.0.0.1:8000"),
		"cloud WebSocket base URL")
	fs.StringVar(&c.CloudHTTP, "cloud-http", envOr("ATOM_CLOUD_HTTP", "http://127.0.0.1:8000"),
		"cloud HTTP base URL")
	fs.StringVar(&c.Token, "token", envOr("ATOM_GATEWAY_TOKEN", "dev-gateway-token"),
		"gateway auth token (change this before deploying)")
	fs.StringVar(&c.MQTTHost, "mqtt-host", envOr("ATOM_MQTT_HOST", "127.0.0.1"), "Mosquitto host")
	fs.IntVar(&c.MQTTPort, "mqtt-port", envIntOr("ATOM_MQTT_PORT", 1883), "Mosquitto port")
	fs.StringVar(&c.DataDir, "data-dir", envOr("ATOM_DATA_DIR", defaultDataDir()),
		"directory for the database, licence config, firmware and logs")
	fs.StringVar(&c.DBPath, "db", "", "local SQLite path (default <data-dir>/store_data.db)")
	fs.StringVar(&c.LicenseConfig, "license-config", "",
		"licence cache path (default <data-dir>/store_license_config.json)")
	fs.StringVar(&c.FirmwareDir, "firmware-dir", "",
		"firmware directory (default <data-dir>/firmware)")
	fs.StringVar(&c.LogFile, "log-file", "",
		"rotating log file (default <data-dir>/logs/gateway.log; '-' disables file logging)")
	fs.StringVar(&c.LogLevel, "log-level", envOr("ATOM_LOG_LEVEL", "info"),
		"debug, info, warn or error")
	fs.IntVar(&c.LogMaxMB, "log-max-mb", 20, "rotate the log at this size")
	fs.IntVar(&c.LogBackups, "log-backups", 5, "rotated log files to keep")
	fs.IntVar(&c.GraceDays, "grace-days", 30,
		"fallback grace window before the cloud has ever answered")
	fs.Float64Var(&licenseIntervalSec, "license-interval", 86400,
		"seconds between authorize calls (default: daily)")
	fs.StringVar(&c.OTAHost, "ota-host", envOr("ATOM_OTA_HOST", "auto"),
		"address the Atom devices use to reach this PC ('auto' detects the LAN IP)")
	fs.IntVar(&c.OTAPort, "ota-port", 8080, "local HTTP OTA port")
	fs.BoolVar(&c.Simulate, "simulate", false,
		"generate Atom Lite traffic locally (no hardware or broker needed)")
	fs.BoolVar(&c.NoMQTT, "no-mqtt", false,
		"do not connect to a broker at all; only meaningful with --simulate")
	fs.IntVar(&c.Devices, "devices", 2, "simulated device count")

	if err := fs.Parse(args); err != nil {
		return c, err
	}

	c.LicenseInterval = time.Duration(licenseIntervalSec * float64(time.Second))
	if c.LicenseInterval < time.Second {
		c.LicenseInterval = time.Second
	}
	if c.Devices < 1 {
		c.Devices = 1
	}

	abs, err := filepath.Abs(c.DataDir)
	if err != nil {
		return c, fmt.Errorf("resolve data dir: %w", err)
	}
	c.DataDir = abs
	c.DBPath = resolvePath(c.DBPath, c.DataDir, "store_data.db")
	c.LicenseConfig = resolvePath(c.LicenseConfig, c.DataDir, "store_license_config.json")
	c.FirmwareDir = resolvePath(c.FirmwareDir, c.DataDir, "firmware")
	if c.LogFile != "-" {
		c.LogFile = resolvePath(c.LogFile, c.DataDir, filepath.Join("logs", "gateway.log"))
	}
	if c.OTAHost == "auto" {
		c.OTAHost = detectLANIP()
	}
	return c, nil
}

func resolvePath(given, dataDir, fallback string) string {
	if given == "" {
		return filepath.Join(dataDir, fallback)
	}
	if filepath.IsAbs(given) {
		return given
	}
	return filepath.Join(dataDir, given)
}

func envIntOr(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

// detectLANIP finds the address an Atom device on the store LAN would use to
// reach this PC. No packet is sent; the UDP dial just picks the route.
func detectLANIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		slog.Warn("could not detect the LAN IP; OTA will advertise 127.0.0.1", "err", err)
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// setupLogging sends structured logs to a rotating file, and to stderr as well
// when a console is attached. A Windows service has no console, so the file is
// the only record -- which is why it is on by default.
func setupLogging(c Config) (io.Closer, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(c.LogLevel))); err != nil {
		level = slog.LevelInfo
	}

	var writers []io.Writer
	var closer io.Closer

	if c.LogFile != "-" {
		if err := os.MkdirAll(filepath.Dir(c.LogFile), 0o755); err != nil {
			return nil, fmt.Errorf("create log dir: %w", err)
		}
		rot := &lumberjack.Logger{
			Filename:   c.LogFile,
			MaxSize:    c.LogMaxMB,
			MaxBackups: c.LogBackups,
			Compress:   true,
		}
		writers = append(writers, rot)
		closer = rot
	}
	if hasConsole() {
		writers = append(writers, os.Stderr)
	}
	if len(writers) == 0 {
		writers = append(writers, io.Discard)
	}

	handler := slog.NewTextHandler(io.MultiWriter(writers...), &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
	return closer, nil
}

func usage() {
	fmt.Fprintf(os.Stderr, `atomair-gateway %s -- Atom Air store gateway

Usage:
  atomair-gateway [flags]              run in the foreground
  atomair-gateway install [flags]      install as a Windows service
  atomair-gateway uninstall            remove the Windows service
  atomair-gateway start|stop|status    control the installed service

Flags passed to 'install' are baked into the service command line.
Run 'atomair-gateway -h' for the full flag list.
`, AppVersion)
}

func main() {
	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "install", "uninstall", "start", "stop", "status", "restart":
			if err := serviceControl(args[0], args[1:]); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			return
		case "help", "--help", "-help":
			usage()
			return
		}
	}

	cfg, err := parseConfig(args)
	if err != nil {
		os.Exit(2)
	}
	closer, err := setupLogging(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if closer != nil {
		defer closer.Close()
	}

	// Under the Windows SCM this hands control to the service dispatcher;
	// everywhere else it reports false and we run in the foreground.
	if handled, err := runAsService(cfg); handled {
		if err != nil {
			slog.Error("service exited with an error", "err", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := NewService(cfg).Run(ctx); err != nil {
		slog.Error("gateway failed", "err", err)
		os.Exit(1)
	}
}
