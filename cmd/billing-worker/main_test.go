package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// buildWorker builds the billing-worker binary into dir and returns its path.
func buildWorker(t *testing.T, dir string) string {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain is not on PATH")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller: cannot locate test source file")
	}
	bin := filepath.Join(dir, "billing-worker")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	cmd.Dir = filepath.Dir(file)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build billing-worker: %v\n%s", err, out)
	}
	return bin
}

// TestStartupLogDoesNotContainPassword starts the worker with a DATABASE_URL
// holding a canary password and checks the canary never appears on stdout or
// stderr. Port 1 refuses fast, so the worker exits at migration time, after
// the startup line is already logged.
func TestStartupLogDoesNotContainPassword(t *testing.T) {
	bin := buildWorker(t, t.TempDir())

	const canary = "sec10canary9f3k7z"
	env := os.Environ()
	env = append(filterEnv(env, "DATABASE_URL"),
		"DATABASE_URL=postgres://billing:"+canary+"@127.0.0.1:1/shop?sslmode=disable")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = env
	out, _ := cmd.CombinedOutput()

	if strings.Contains(string(out), canary) {
		t.Errorf("startup output contains the database password:\n%s", out)
	}
	if !strings.Contains(string(out), "connecting to database") {
		t.Errorf("startup output lacks the static connecting line:\n%s", out)
	}
}

// filterEnv drops entries with the given key from env.
func filterEnv(env []string, key string) []string {
	kept := env[:0]
	prefix := key + "="
	for _, e := range env {
		if !strings.HasPrefix(e, prefix) {
			kept = append(kept, e)
		}
	}
	return kept
}
