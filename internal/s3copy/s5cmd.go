package s3copy

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Copier struct {
	Binary   string
	Endpoint string
}

func (c Copier) UploadDir(ctx context.Context, localDir, bucket, prefix string) error {
	if err := requireDir(localDir); err != nil {
		return err
	}
	return c.run(ctx, "cp", withTrailingSeparator(localDir), s3URI(bucket, prefix)+"/")
}

func (c Copier) DownloadPrefix(ctx context.Context, bucket, prefix, localDir string) error {
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		return err
	}
	return c.run(ctx, "cp", s3URI(bucket, prefix)+"/*", withTrailingSeparator(localDir))
}

func (c Copier) run(ctx context.Context, command string, args ...string) error {
	binary := c.Binary
	if strings.TrimSpace(binary) == "" {
		binary = "s5cmd"
	}
	fullArgs := []string{"--retry-count", "0"}
	if c.Endpoint != "" {
		fullArgs = append(fullArgs, "--endpoint-url", c.Endpoint)
	}
	fullArgs = append(fullArgs, command)
	fullArgs = append(fullArgs, args...)
	cmd := exec.CommandContext(ctx, binary, fullArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s failed: %w: %s", binary, strings.Join(fullArgs, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
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

func withTrailingSeparator(path string) string {
	clean := filepath.Clean(path)
	if strings.HasSuffix(clean, string(filepath.Separator)) {
		return clean
	}
	return clean + string(filepath.Separator)
}
