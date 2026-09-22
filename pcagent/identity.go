package main

import (
	"fmt"
	"net"
)

// detectMAC picks a stable hardware address to identify this PC by,
// preferring a real, active, non-virtual network interface. PC방 client PCs
// are usually cloned from one disk image, so hostnames cannot be trusted to
// be unique, but every NIC still has a distinct burned-in MAC.
func detectMAC() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("list network interfaces: %w", err)
	}

	pick := func(want func(net.Interface) bool) string {
		for _, ifc := range ifaces {
			if len(ifc.HardwareAddr) == 0 || !want(ifc) {
				continue
			}
			return ifc.HardwareAddr.String()
		}
		return ""
	}

	isUp := func(ifc net.Interface) bool { return ifc.Flags&net.FlagUp != 0 }
	isLoopback := func(ifc net.Interface) bool { return ifc.Flags&net.FlagLoopback != 0 }

	// Prefer an interface that is both up and not loopback; fall back to any
	// non-loopback interface with a MAC if none is up yet (the agent can start
	// moments after boot, before the NIC has come up).
	if mac := pick(func(ifc net.Interface) bool { return isUp(ifc) && !isLoopback(ifc) }); mac != "" {
		return mac, nil
	}
	if mac := pick(func(ifc net.Interface) bool { return !isLoopback(ifc) }); mac != "" {
		return mac, nil
	}
	return "", fmt.Errorf("no network interface has a hardware address")
}
