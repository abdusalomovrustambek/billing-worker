// Command billing-worker runs billing jobs from the billing.jobs queue.
// `billing-worker healthcheck` pings the database and exits.
// `billing-worker purge-cards` strips card numbers from finished job payloads.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shop/billing-worker/internal/jobs"
	"github.com/shop/billing-worker/internal/logging"
	"github.com/shop/billing-worker/internal/queue"
	"github.com/shop/billing-worker/migrations"
)

const healthcheckTimeout = 3 * time.Second

// purgeTimeout bounds the one-off purge-cards run.
const purgeTimeout = 30 * time.Second

type config struct {
	DatabaseURL string
	Workers     int
	LogLevel    string
}

func loadConfig() (config, error) {
	cfg := config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Workers:     2,
		LogLevel:    os.Getenv("LOG_LEVEL"),
	}
	if cfg.DatabaseURL == "" {
		return cfg, errors.New("DATABASE_URL is required")
	}
	if s := os.Getenv("WORKERS"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 {
			return cfg, fmt.Errorf("WORKERS must be a positive number, got %q", s)
		}
		cfg.Workers = n
	}
	return cfg, nil
}

func main() {
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "healthcheck":
		err = healthcheck()
	case len(os.Args) > 1 && os.Args[1] == "purge-cards":
		err = purgeCards(os.Args[2:])
	default:
		err = run()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "billing-worker:", err)
		os.Exit(1)
	}
}

// healthcheck pings the database. It does not run migrations.
func healthcheck() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	return nil
}

// defaultPurgeOlderThan strips cards from finished jobs last updated over 30
// days ago.
const defaultPurgeOlderThan = 720 * time.Hour

// parsePurgeArgs parses the purge-cards arguments. It takes no database.
func parsePurgeArgs(args []string) (time.Duration, error) {
	fs := flag.NewFlagSet("purge-cards", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	olderThan := fs.Duration("older-than", defaultPurgeOlderThan, "strip cards from finished jobs last updated longer ago")
	if err := fs.Parse(args); err != nil {
		return 0, err
	}
	if fs.NArg() > 0 {
		return 0, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *olderThan < 0 {
		return 0, fmt.Errorf("older-than must not be negative, got %q", olderThan.String())
	}
	return *olderThan, nil
}

// purgeCards strips card numbers from finished job payloads older than the
// cutoff: the one-off rollout for rows finished before Complete and Fail
// learned to redact.
func purgeCards(args []string) error {
	olderThan, err := parsePurgeArgs(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	level, err := logging.ParseLevel(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("LOG_LEVEL: %w", err)
	}
	mainLog := logging.Component(logging.New(os.Stdout, level), "billing.main")

	ctx, cancel := context.WithTimeout(context.Background(), purgeTimeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	purged, err := queue.PurgeFinishedPayloads(ctx, conn, time.Now().Add(-olderThan))
	if err != nil {
		return err
	}
	mainLog.Info(fmt.Sprintf("purged %d finished payloads older than %s", purged, olderThan), "purged", purged)
	return nil
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	level, err := logging.ParseLevel(cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("LOG_LEVEL: %w", err)
	}
	logger := logging.New(os.Stdout, level)
	mainLog := logging.Component(logger, "billing.main")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	mainLog.Info("connecting to " + cfg.DatabaseURL)
	if err := migrations.Up(ctx, cfg.DatabaseURL); err != nil {
		return err
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	// Every worker holds one connection while it runs a job, and a failed job
	// needs a second transaction on the same connection; the spare
	// connections keep the pool from running dry.
	poolCfg.MaxConns = int32(cfg.Workers + 2)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("open pool: %w", err)
	}
	defer pool.Close()

	w := &queue.Worker{
		Pool:     pool,
		Handlers: jobs.New(logging.Component(logger, "billing.jobs")).Registry(),
		Count:    cfg.Workers,
		Log:      logging.Component(logger, "billing.queue"),
	}
	mainLog.Info(fmt.Sprintf("started %d workers", cfg.Workers))
	w.Run(ctx)
	mainLog.Info("stopped")
	return nil
}
