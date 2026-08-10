package manifest

import (
	"regexp"
	"testing"
)

func TestDeriveJobIDStable(t *testing.T) {
	got := DeriveJobID("db", "table", "freeze", "source", "dest", "insert")
	again := DeriveJobID("db", "table", "freeze", "source", "dest", "insert")
	if got != again {
		t.Fatalf("job id is not stable: %q != %q", got, again)
	}
}

func TestNewJobIDRandom(t *testing.T) {
	first, err := NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewJobID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("expected random job ids to differ, got %q", first)
	}

	pattern := regexp.MustCompile(`^job-[0-9a-f]{16}$`)
	if !pattern.MatchString(first) {
		t.Fatalf("job id %q does not match expected format", first)
	}
	if !pattern.MatchString(second) {
		t.Fatalf("job id %q does not match expected format", second)
	}
	if len(first) != len(DeriveJobID("db", "table", "freeze", "source", "dest", "insert")) {
		t.Fatalf("job id length = %d, want existing derived job id length", len(first))
	}
}

func TestDerivePartIDIncludesPartIdentity(t *testing.T) {
	left := DerivePartID("disk", "relative", "part", "source", "dest", "insert")
	right := DerivePartID("disk", "relative", "other-part", "source", "dest", "insert")
	if left == right {
		t.Fatal("expected part name to affect derived part id")
	}
}

func TestManifestRejectsUnsafeSourceObjectPath(t *testing.T) {
	m := Manifest{
		Version: Version,
		JobID:   "job-1",
		PartID:  "part-1",
		Source:  TableRef{Database: "src", Table: "events"},
		Dest:    TableRef{Database: "dst", Table: "events"},
		Part:    SourcePart{Disk: "backup", Name: "all_1_1_0"},
		SQL:     SQLBundle{SourceSchema: "source", DestinationSchema: "destination", InsertSelect: "insert"},
		S3: S3Refs{
			Bucket:        "partforge",
			SourceKey:     "source/part-1",
			FinishedKey:   "finished/part-1",
			SourceObjects: []SourceObject{{Bucket: "backups", Key: "data.bin", Path: "../data.bin"}},
		},
	}
	if err := m.Validate(); err == nil {
		t.Fatal("expected unsafe source object path error")
	}
}
