//go:build windows

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	serviceName    = "AtomAirGateway"
	serviceDisplay = "Atom Air Store Gateway"
	serviceDesc    = "Collects Atom Lite sensor data over MQTT, stores it locally, " +
		"and relays on-demand live streams and AC control to the Atom Air cloud."
	stopTimeout = 20 * time.Second
)

// winService adapts Service to the Windows service control manager.
type winService struct{ cfg Config }

func (w *winService) Execute(_ []string, r <-chan svc.ChangeRequest,
	status chan<- svc.Status) (bool, uint32) {

	const accepted = svc.AcceptStop | svc.AcceptShutdown
	status <- svc.Status{State: svc.StartPending}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- NewService(w.cfg).Run(ctx) }()

	status <- svc.Status{State: svc.Running, Accepts: accepted}
	slog.Info("running under the Windows service control manager", "service", serviceName)

	for {
		select {
		case req := <-r:
			switch req.Cmd {
			case svc.Interrogate:
				status <- req.CurrentStatus
			case svc.Stop, svc.Shutdown:
				slog.Info("stop requested by the SCM")
				status <- svc.Status{State: svc.StopPending}
				cancel()
				select {
				case <-done:
				case <-time.After(stopTimeout):
					slog.Warn("shutdown timed out; exiting anyway", "timeout", stopTimeout)
				}
				return false, 0
			default:
				slog.Warn("unexpected service control request", "cmd", req.Cmd)
			}
		case err := <-done:
			if err != nil {
				slog.Error("gateway stopped with an error", "err", err)
				return false, 1
			}
			return false, 0
		}
	}
}

// runAsService hands control to the SCM when we were launched as a service.
func runAsService(cfg Config) (bool, error) {
	inService, err := svc.IsWindowsService()
	if err != nil {
		return false, fmt.Errorf("detect service context: %w", err)
	}
	if !inService {
		return false, nil
	}
	return true, svc.Run(serviceName, &winService{cfg: cfg})
}

// hasConsole reports whether stderr logging is useful. A service has no
// console, so it logs to file only.
func hasConsole() bool {
	inService, err := svc.IsWindowsService()
	if err != nil {
		return true
	}
	return !inService
}

// connectSCM opens the service control manager with only the rights the
// action needs, so read-only commands like `status` work without elevation.
func connectSCM(write bool) (*mgr.Mgr, error) {
	access := uint32(windows.SC_MANAGER_CONNECT | windows.SC_MANAGER_ENUMERATE_SERVICE)
	if write {
		access = windows.SC_MANAGER_ALL_ACCESS
	}
	h, err := windows.OpenSCManager(nil, nil, access)
	if err != nil {
		if write {
			return nil, fmt.Errorf("connect to the service manager "+
				"(run this from an Administrator prompt): %w", err)
		}
		return nil, fmt.Errorf("connect to the service manager: %w", err)
	}
	return &mgr.Mgr{Handle: h}, nil
}

// openService opens the gateway service with the rights the action needs.
func openService(m *mgr.Mgr, write bool) (*mgr.Service, error) {
	access := uint32(windows.SERVICE_QUERY_STATUS | windows.SERVICE_QUERY_CONFIG)
	if write {
		access |= windows.SERVICE_START | windows.SERVICE_STOP |
			windows.SERVICE_CHANGE_CONFIG | windows.DELETE
	}
	name, err := windows.UTF16PtrFromString(serviceName)
	if err != nil {
		return nil, err
	}
	h, err := windows.OpenService(m.Handle, name, access)
	if err != nil {
		return nil, err
	}
	return &mgr.Service{Name: serviceName, Handle: h}, nil
}

func serviceControl(action string, args []string) error {
	// Validate flags before touching the SCM, so a typo reports the typo
	// rather than an access-denied error.
	var cfg Config
	if action == "install" {
		var err error
		if cfg, err = parseConfig(args); err != nil {
			return fmt.Errorf("invalid flags: %w", err)
		}
	} else if len(args) > 0 {
		return fmt.Errorf("%q takes no flags (got %v)", action, args)
	}

	m, err := connectSCM(action != "status")
	if err != nil {
		return err
	}
	defer m.Disconnect()

	switch action {
	case "install":
		return installService(m, cfg, args)
	case "uninstall":
		return uninstallService(m)
	case "start":
		return startService(m)
	case "stop":
		return stopService(m)
	case "restart":
		if err := stopService(m); err != nil {
			slog.Warn("stop before restart failed", "err", err)
		}
		return startService(m)
	case "status":
		return printStatus(m)
	}
	return fmt.Errorf("unknown action %q", action)
}

func installService(m *mgr.Mgr, cfg Config, args []string) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir %s: %w", cfg.DataDir, err)
	}

	exe, err := os.Executable()
	if err != nil {
		return err
	}

	if s, err := openService(m, false); err == nil {
		s.Close()
		return fmt.Errorf("service %s already exists (run 'uninstall' first)", serviceName)
	}

	s, err := m.CreateService(serviceName, exe, mgr.Config{
		DisplayName:      serviceDisplay,
		Description:      serviceDesc,
		StartType:        mgr.StartAutomatic,
		DelayedAutoStart: true, // let the network stack come up first
		Dependencies:     []string{"Tcpip"},
	}, args...)
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	defer s.Close()

	// Restart on crash: 5s, 15s, then every 60s; reset the counter daily.
	if err := s.SetRecoveryActions([]mgr.RecoveryAction{
		{Type: mgr.ServiceRestart, Delay: 5 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 15 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 60 * time.Second},
	}, uint32((24 * time.Hour).Seconds())); err != nil {
		slog.Warn("could not set service recovery actions", "err", err)
	}

	fmt.Printf("installed %s\n", serviceName)
	fmt.Printf("  binary    %s\n", windows.ComposeCommandLine(append([]string{exe}, args...)))
	fmt.Printf("  data dir  %s\n", cfg.DataDir)
	fmt.Printf("  log file  %s\n", cfg.LogFile)
	fmt.Printf("  OTA       http://%s:%d/\n", cfg.OTAHost, cfg.OTAPort)

	addFirewallRule(cfg.OTAPort)
	fmt.Printf("\nstart it with:  %s start\n", exe)
	return nil
}

// addFirewallRule opens the OTA port so Atom devices on the store LAN can pull
// firmware. Failure is reported, not fatal -- an operator may manage the
// firewall by policy instead.
func addFirewallRule(port int) {
	rule := fmt.Sprintf("AtomAir OTA %d", port)
	// Remove any stale rule first so repeated installs do not stack duplicates.
	_ = exec.Command("netsh", "advfirewall", "firewall", "delete", "rule",
		"name="+rule).Run()

	cmd := exec.Command("netsh", "advfirewall", "firewall", "add", "rule",
		"name="+rule, "dir=in", "action=allow", "protocol=TCP",
		"localport="+itoa(port), "profile=private,domain")
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("  firewall  could not add an inbound rule for TCP %d: %v\n", port, err)
		fmt.Printf("            %s\n", out)
		return
	}
	fmt.Printf("  firewall  inbound TCP %d allowed (private, domain)\n", port)
}

func uninstallService(m *mgr.Mgr) error {
	s, err := openService(m, true)
	if err != nil {
		return fmt.Errorf("service %s is not installed", serviceName)
	}
	defer s.Close()

	if st, err := s.Query(); err == nil && st.State != svc.Stopped {
		if _, err := s.Control(svc.Stop); err != nil {
			slog.Warn("could not stop the service before removing it", "err", err)
		} else {
			waitForState(s, svc.Stopped, stopTimeout)
		}
	}
	if err := s.Delete(); err != nil {
		return fmt.Errorf("delete service: %w", err)
	}
	fmt.Printf("removed %s\n", serviceName)
	return nil
}

func startService(m *mgr.Mgr) error {
	s, err := openService(m, true)
	if err != nil {
		return fmt.Errorf("service %s is not installed", serviceName)
	}
	defer s.Close()
	if err := s.Start(); err != nil {
		return fmt.Errorf("start service: %w", err)
	}
	fmt.Printf("%s starting\n", serviceName)
	return nil
}

func stopService(m *mgr.Mgr) error {
	s, err := openService(m, true)
	if err != nil {
		return fmt.Errorf("service %s is not installed", serviceName)
	}
	defer s.Close()
	if _, err := s.Control(svc.Stop); err != nil {
		return fmt.Errorf("stop service: %w", err)
	}
	waitForState(s, svc.Stopped, stopTimeout)
	fmt.Printf("%s stopped\n", serviceName)
	return nil
}

func printStatus(m *mgr.Mgr) error {
	s, err := openService(m, false)
	if err != nil {
		fmt.Printf("%s: not installed\n", serviceName)
		return nil
	}
	defer s.Close()

	st, err := s.Query()
	if err != nil {
		return err
	}
	cfg, err := s.Config()
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s\n", serviceName, stateName(st.State))
	fmt.Printf("  binary  %s\n", cfg.BinaryPathName)
	fmt.Printf("  start   %s (delayed=%v)\n", startTypeName(cfg.StartType), cfg.DelayedAutoStart)
	if st.ProcessId != 0 {
		fmt.Printf("  pid     %d\n", st.ProcessId)
	}
	return nil
}

func waitForState(s *mgr.Service, want svc.State, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st, err := s.Query()
		if err != nil || st.State == want {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func stateName(s svc.State) string {
	switch s {
	case svc.Stopped:
		return "stopped"
	case svc.StartPending:
		return "starting"
	case svc.StopPending:
		return "stopping"
	case svc.Running:
		return "running"
	case svc.Paused:
		return "paused"
	}
	return fmt.Sprintf("state %d", s)
}

func startTypeName(t uint32) string {
	switch t {
	case mgr.StartAutomatic:
		return "automatic"
	case mgr.StartManual:
		return "manual"
	case mgr.StartDisabled:
		return "disabled"
	}
	return fmt.Sprintf("type %d", t)
}
