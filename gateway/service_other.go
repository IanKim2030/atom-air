//go:build !windows

package main

import "fmt"

// The gateway targets Windows store PCs; on other platforms it runs only in
// the foreground, which is what development and CI need.

func runAsService(Config) (bool, error) { return false, nil }

func hasConsole() bool { return true }

func serviceControl(action string, _ []string) error {
	return fmt.Errorf("%q is only available on Windows; run the binary in the foreground instead",
		action)
}
