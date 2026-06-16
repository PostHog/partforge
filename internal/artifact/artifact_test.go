package artifact

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/partforge/partforge/internal/manifest"
)

func TestManifestRoundTrip(t *testing.T) {
	root := t.TempDir()
	m := manifest.Manifest{
		Version:   manifest.Version,
		JobID:     "job-1",
		PartID:    "part-1",
		Freeze:    "freeze-1",
		Source:    manifest.TableRef{Database: "src", Table: "t"},
		Dest:      manifest.TableRef{Database: "dst", Table: "t2"},
		Part:      manifest.SourcePart{Disk: "default", Name: "all_1_1_0", RelativePath: "store/x/all_1_1_0"},
		SQL:       manifest.SQLBundle{SourceSchema: "CREATE TABLE src.t (x UInt64) ENGINE = MergeTree ORDER BY x", DestinationSchema: "CREATE TABLE dst.t2 (x UInt64) ENGINE = MergeTree ORDER BY x", InsertSelect: "INSERT INTO dst.t2 SELECT * FROM src.t"},
		S3:        manifest.S3Refs{Bucket: "bucket", SourceKey: "source/part-1", FinishedKey: "finished/part-1"},
		CreatedAt: time.Now(),
	}

	if err := WriteManifest(root, m); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ManifestName)); err != nil {
		t.Fatal(err)
	}

	got, err := ReadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if got.JobID != m.JobID || got.Part.Name != m.Part.Name || got.Part.Disk != m.Part.Disk {
		t.Fatalf("unexpected manifest: %+v", got)
	}
}
