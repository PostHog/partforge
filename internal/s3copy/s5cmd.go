package s3copy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	s5cmdRetries = 3
)

var s5cmdRetryBaseDelay = time.Second

type Copier struct {
	Binary     string
	Endpoint   string
	NumWorkers int
}

type CommandError struct {
	Binary string
	Args   []string
	Err    error
	Output string
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("%s %s failed: %v: %s", e.Binary, strings.Join(e.Args, " "), e.Err, strings.TrimSpace(e.Output))
}

func (e *CommandError) Unwrap() error {
	return e.Err
}

func (c Copier) UploadDir(ctx context.Context, localDir, bucket, prefix string) error {
	if err := requireDir(localDir); err != nil {
		return err
	}
	return c.runS5cmd(ctx, c.copyArgs(withTrailingSeparator(localDir), s3URI(bucket, prefix)+"/"), nil)
}

func (c Copier) UploadGlob(ctx context.Context, localGlob, bucket, prefix string) error {
	return c.runS5cmd(ctx, c.copyArgs(localGlob, s3URI(bucket, prefix)+"/"), nil)
}

func (c Copier) DownloadPrefix(ctx context.Context, bucket, prefix, localDir string) error {
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return err
	}
	return c.runS5cmd(ctx, c.copyArgs(s3URI(bucket, prefix)+"/*", withTrailingSeparator(localDir)), nil)
}

func (c Copier) DownloadFile(ctx context.Context, bucket, key, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return err
	}
	return c.runS5cmd(ctx, c.copyArgs(s3URI(bucket, key), localPath), nil)
}

func (c Copier) DeletePrefix(ctx context.Context, bucket, prefix string) error {
	target, err := deletePrefixTarget(bucket, prefix)
	if err != nil {
		return err
	}
	return c.runS5cmd(ctx, c.deleteArgs(target), nil)
}

func (c Copier) DeletePrefixIfExists(ctx context.Context, bucket, prefix string) error {
	target, err := deletePrefixTarget(bucket, prefix)
	if err != nil {
		return err
	}
	return c.runS5cmd(ctx, c.deleteArgs(target), isNoObjectFound)
}

func (c Copier) runS5cmd(ctx context.Context, fullArgs []string, acceptErr func(error) bool) error {
	maxAttempts := s5cmdRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := c.runArgs(ctx, fullArgs)
		if err == nil || (acceptErr != nil && acceptErr(err)) {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		lastErr = err
		if attempt < maxAttempts {
			delay := s5cmdCommandRetryDelay(attempt)
			slog.Warn(
				"s5cmd command failed; retrying",
				"attempt", attempt,
				"next_attempt", attempt+1,
				"max_attempts", maxAttempts,
				"retry_delay", delay,
				"error", err,
			)
			if err := sleepContext(ctx, delay); err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("s5cmd command failed after %d attempts: %w", maxAttempts, lastErr)
}

func (c Copier) runArgs(ctx context.Context, fullArgs []string) error {
	binary := c.Binary
	if strings.TrimSpace(binary) == "" {
		binary = "s5cmd"
	}
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return &CommandError{Binary: binary, Args: fullArgs, Err: err, Output: string(out)}
	}
	return nil
}

func isNoObjectFound(err error) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && strings.Contains(commandErr.Output, "no object found")
}

func (c Copier) deleteArgs(target string) []string {
	return c.args("rm", target)
}

func (c Copier) copyArgs(args ...string) []string {
	return c.args("cp", args...)
}

func (c Copier) args(command string, args ...string) []string {
	fullArgs := []string{"--log=error", "--retry-count", fmt.Sprintf("%d", s5cmdRetries)}
	if c.NumWorkers > 0 {
		fullArgs = append(fullArgs, "--numworkers", fmt.Sprintf("%d", c.NumWorkers))
	}
	if c.Endpoint != "" {
		fullArgs = append(fullArgs, "--endpoint-url", c.Endpoint)
	}
	fullArgs = append(fullArgs, command)
	fullArgs = append(fullArgs, args...)
	return fullArgs
}

func s5cmdCommandRetryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return s5cmdRetryBaseDelay
	}
	return s5cmdRetryBaseDelay << (attempt - 1)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func requireDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", path)
	}
	return nil
}

func s3URI(bucket, prefix string) string {
	return "s3://" + strings.Trim(bucket, "/") + "/" + strings.Trim(prefix, "/")
}

func deletePrefixTarget(bucket, prefix string) (string, error) {
	bucket = strings.Trim(bucket, "/")
	prefix = strings.Trim(prefix, "/")
	if bucket == "" {
		return "", fmt.Errorf("s3 bucket is required")
	}
	if prefix == "" {
		return "", fmt.Errorf("s3 prefix is required")
	}
	if containsS5cmdGlobMeta(bucket) {
		return "", fmt.Errorf("s3 bucket %q contains s5cmd glob metacharacters", bucket)
	}
	if containsS5cmdGlobMeta(prefix) {
		return "", fmt.Errorf("s3 prefix %q contains s5cmd glob metacharacters", prefix)
	}
	return s3URI(bucket, prefix) + "/*", nil
}

func containsS5cmdGlobMeta(value string) bool {
	return strings.ContainsAny(value, "*?[]{}")
}

func withTrailingSeparator(path string) string {
	clean := filepath.Clean(path)
	if strings.HasSuffix(clean, string(filepath.Separator)) {
		return clean
	}
	return clean + string(filepath.Separator)
}
