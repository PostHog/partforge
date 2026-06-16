package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/partforge/partforge/internal/state"
)

func TestSummarizeJob(t *testing.T) {
	summary := summarizeJob("job-1", []state.Part{
		{PartID: "part-1", Status: state.StatusImported},
		{PartID: "part-2", Status: state.StatusFinished},
		{PartID: "part-3", Status: state.StatusFailed, Error: "boom"},
	})

	if summary.Status != "FAILED" {
		t.Fatalf("status = %s", summary.Status)
	}
	if summary.Total != 3 {
		t.Fatalf("total = %d", summary.Total)
	}
	if summary.RewriteCompleted != 2 {
		t.Fatalf("rewrite completed = %d", summary.RewriteCompleted)
	}
	if summary.ImportCompleted != 1 {
		t.Fatalf("import completed = %d", summary.ImportCompleted)
	}
	if len(summary.FailedParts) != 1 || summary.FailedParts[0].PartID != "part-3" {
		t.Fatalf("failed parts = %+v", summary.FailedParts)
	}
}

func TestSelectRetryParts(t *testing.T) {
	parts := []state.Part{
		{PartID: "part-1", Status: state.StatusFailed},
		{PartID: "part-2", Status: state.StatusImported},
		{PartID: "part-3", Status: state.StatusFailed},
	}

	all, err := selectRetryParts(parts, true, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all len = %d", len(all))
	}

	forced, err := selectRetryParts(parts, true, true, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(forced) != 3 {
		t.Fatalf("forced len = %d", len(forced))
	}

	one, err := selectRetryParts(parts, false, false, "part-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].PartID != "part-1" {
		t.Fatalf("one = %+v", one)
	}

	if _, err := selectRetryParts(parts, false, false, "part-2"); err == nil {
		t.Fatal("expected non-failed part error")
	}
}

func TestApplyConfigDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeTestFile(t, path, `{
  "bucket": "global-bucket",
  "aws_region": "us-west-2",
  "commands": {
    "upload-freeze": {
      "bucket": "upload-bucket",
      "prefix": "uploads"
    }
  }
}`)

	fs := flag.NewFlagSet("upload-freeze", flag.ContinueOnError)
	bucket := fs.String("bucket", "", "")
	prefix := fs.String("prefix", "partforge", "")
	region := fs.String("aws-region", "us-east-1", "")
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigDefaults(fs, path, "upload-freeze"); err != nil {
		t.Fatal(err)
	}

	if *bucket != "upload-bucket" {
		t.Fatalf("bucket = %q", *bucket)
	}
	if *prefix != "uploads" {
		t.Fatalf("prefix = %q", *prefix)
	}
	if *region != "us-west-2" {
		t.Fatalf("region = %q", *region)
	}
}

func TestApplyConfigDefaultsDoesNotOverrideCLI(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	writeTestFile(t, path, `{"bucket": "config-bucket"}`)

	fs := flag.NewFlagSet("upload-freeze", flag.ContinueOnError)
	bucket := fs.String("bucket", "", "")
	if err := fs.Parse([]string{"-bucket=cli-bucket"}); err != nil {
		t.Fatal(err)
	}
	if err := applyConfigDefaults(fs, path, "upload-freeze"); err != nil {
		t.Fatal(err)
	}

	if *bucket != "cli-bucket" {
		t.Fatalf("bucket = %q", *bucket)
	}
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
