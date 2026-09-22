// Command atomair-pcagent runs on one PC방 client PC and reports whether it
// is powered on to the store gateway, over the same local Mosquitto broker
// the Atom Lite devices use.
//
//	PC (this agent) --MQTT (Mosquitto)--> atomair-gateway
//
// The only thing this binary needs configured is the gateway's Mosquitto
// address. It identifies itself by its own MAC address (auto-detected, cached
// after the first run) and otherwise knows nothing -- not its room, not the
// store id, not even its own name. All of that mapping lives entirely in the
// gateway's room_config.json (see gateway/roomconfig.go and
// docs/PC_AGENT.md): an admin adds this PC's MAC under whichever room it
// physically sits in, and the gateway does the rest. The gateway (see
// gateway/pcstatus.go) turns a room going fully quiet into an automatic
// AC-off.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// AppVersion is written to the log on startup.
const AppVersion = "1.0.0"

// Config is the fully resolved runtime configuration.
type Config struct {
	MQTTHost   string
	MQTTPort   int
	PCKey      string // this PC's identity, normally its MAC address
	DataDir    string
	ConfigPath string
	Heartbeat  time.Duration
	LogFile    string
	LogLevel   string
	LogMaxMB   int
	LogBackups int
}

// persisted is the subset of Config cached to disk, so a restarted or
// re-installed service does not need any flags typed in again -- including
// the auto-detected identity, which is cached rather than re-detected every
// run so it stays stable even if the OS ever reorders network interfaces.
type persisted struct {
	MQTTHost string `json:"mqtt_host"`
	MQTTPort int    `json:"mqtt_port"`
	PCKey    string `json:"pc_key"`
}

func defaultDataDir() string {
	// A Windows service runs as LocalSystem with its working directory set to
	// system32, and Program Files is not writable. ProgramData is the correct
	// home for service state.
	if dir := os.Getenv("ProgramData"); dir != "" {
		return filepath.Join(dir, "AtomAirPCAgent")
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

func envIntOr(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return fallback
}

func loadPersisted(path string) persisted {
	var p persisted
	data, err := os.ReadFile(path)
	if err != nil {
		return p
	}
	_ = json.Unmarshal(data, &p)
	return p
}

func savePersisted(path string, p persisted) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// parseConfig builds the config from flags (falling back to a cached previous
// run, then to defaults), resolves this PC's identity, and resolves every
// path so nothing depends on the process working directory. On success it
// re-caches everything, including the resolved identity.
func parseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("atomair-pcagent", flag.ContinueOnError)
	var c Config
	var heartbeatSec float64
	var pcKeyFlag string

	dataDirDefault := envOr("ATOM_DATA_DIR", defaultDataDir())
	cached := loadPersisted(filepath.Join(dataDirDefault, "pcagent_config.json"))

	mqttHostDefault := cached.MQTTHost
	if mqttHostDefault == "" {
		mqttHostDefault = "127.0.0.1"
	}
	fs.StringVar(&c.MQTTHost, "mqtt-host", envOr("ATOM_MQTT_HOST", mqttHostDefault),
		"the store gateway PC's Mosquitto host -- the only thing this agent needs configured")
	mqttPortDefault := cached.MQTTPort
	if mqttPortDefault == 0 {
		mqttPortDefault = 1883
	}
	fs.IntVar(&c.MQTTPort, "mqtt-port", envIntOr("ATOM_MQTT_PORT", mqttPortDefault),
		"the store gateway PC's Mosquitto port")
	fs.StringVar(&pcKeyFlag, "pc-key", envOr("ATOM_PC_KEY", ""),
		"override this PC's identity key (default: auto-detected MAC address, "+
			"cached under --data-dir after the first run)")
	fs.StringVar(&c.DataDir, "data-dir", dataDirDefault,
		"directory for the local identity cache and logs")
	fs.Float64Var(&heartbeatSec, "heartbeat", 30,
		"seconds between on-status heartbeats")
	fs.StringVar(&c.LogFile, "log-file", "",
		"rotating log file (default <data-dir>/logs/pcagent.log; '-' disables file logging)")
	fs.StringVar(&c.LogLevel, "log-level", envOr("ATOM_LOG_LEVEL", "info"),
		"debug, info, warn or error")
	fs.IntVar(&c.LogMaxMB, "log-max-mb", 10, "rotate the log at this size")
	fs.IntVar(&c.LogBackups, "log-backups", 3, "rotated log files to keep")

	if err := fs.Parse(args); err != nil {
		return c, err
	}

	c.PCKey = pcKeyFlag
	if c.PCKey == "" {
		c.PCKey = cached.PCKey
	}
	if c.PCKey == "" {
		mac, err := detectMAC()
		if err != nil {
			return c, fmt.Errorf("could not determine this PC's identity automatically (%w); "+
				"pass --pc-key to set one manually", err)
		}
		c.PCKey = mac
	}

	c.Heartbeat = time.Duration(heartbeatSec * float64(time.Second))
	if c.Heartbeat < time.Second {
		c.Heartbeat = time.Second
	}

	abs, err := filepath.Abs(c.DataDir)
	if err != nil {
		return c, fmt.Errorf("resolve data dir: %w", err)
	}
	c.DataDir = abs
	c.ConfigPath = filepath.Join(c.DataDir, "pcagent_config.json")
	if c.LogFile != "-" {
		c.LogFile = resolvePath(c.LogFile, c.DataDir, filepath.Join("logs", "pcagent.log"))
	}

	if err := savePersisted(c.ConfigPath, persisted{
		MQTTHost: c.MQTTHost, MQTTPort: c.MQTTPort, PCKey: c.PCKey,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not cache identity: %v\n", err)
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

// setupLogging sends structured logs to a rotating file, and to stderr as
// well when a console is attached. A Windows service has no console, so the
// file is the only record.
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
	fmt.Fprintf(os.Stderr, `atomair-pcagent %s -- Atom Air PC방 client-PC power reporter

Usage:
  atomair-pcagent [flags]              run in the foreground
  atomair-pcagent install [flags]      install as a Windows service
  atomair-pcagent uninstall            remove the Windows service
  atomair-pcagent start|stop|status    control the installed service

The only flag you normally need is --mqtt-host (the store gateway PC's
address). This agent identifies itself by its own MAC address and reports
only on/off; the gateway's room_config.json is what assigns a MAC to a room.

Flags passed to 'install' are baked into the service command line.
Run 'atomair-pcagent -h' for the full flag list.
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
		fmt.Fprintln(os.Stderr, "error:", err)
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
			slog.Error("pcagent exited with an error", "err", err)
			os.Exit(1)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("pcagent starting", "pc_key", cfg.PCKey,
		"broker", fmt.Sprintf("%s:%d", cfg.MQTTHost, cfg.MQTTPort), "version", AppVersion)
	slog.Info("if the gateway has not been told about this PC yet, "+
		"add pc_key to a room in its room_config.json", "pc_key", cfg.PCKey)
	if err := NewAgent(cfg).Run(ctx); err != nil {
		slog.Error("pcagent failed", "err", err)
		os.Exit(1)
	}
}
