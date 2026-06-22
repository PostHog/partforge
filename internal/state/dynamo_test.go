package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func TestResolveDynamoRegionKeepsResolvedRegion(t *testing.T) {
	got := resolveDynamoRegion(
		context.Background(),
		aws.Config{Region: "eu-central-1"},
		defaultRegion,
		func(context.Context, aws.Config) (string, error) {
			t.Fatal("should not call IMDS when a region is already resolved")
			return "", nil
		},
	)

	if got != "eu-central-1" {
		t.Fatalf("region = %q", got)
	}
}

func TestResolveDynamoRegionUsesIMDS(t *testing.T) {
	got := resolveDynamoRegion(
		context.Background(),
		aws.Config{},
		defaultRegion,
		func(context.Context, aws.Config) (string, error) {
			return "eu-central-1", nil
		},
	)

	if got != "eu-central-1" {
		t.Fatalf("region = %q", got)
	}
}

func TestResolveDynamoRegionFallsBackWhenIMDSUnavailable(t *testing.T) {
	got := resolveDynamoRegion(
		context.Background(),
		aws.Config{},
		defaultRegion,
		func(context.Context, aws.Config) (string, error) {
			return "", errors.New("imds unavailable")
		},
	)

	if got != defaultRegion {
		t.Fatalf("region = %q", got)
	}
}

func TestProgressRemoveExpressionCoversRewriteMetadata(t *testing.T) {
	expr := progressRemoveExpression()
	for _, attr := range []string{
		"progress_updated_at",
		"read_rows",
		"read_bytes",
		"written_rows",
		"written_bytes",
		"source_active_part_count",
		"source_active_part_rows",
		"source_active_part_bytes",
		"destination_active_part_count",
		"destination_active_part_rows",
		"destination_active_part_bytes",
		"destination_failed_merges",
		"rewrite_stage",
		"rewrite_stage_started_at",
		"rewrite_stage_elapsed_ms",
		"rewrite_total_elapsed_ms",
		"rewrite_stage_durations_ms",
	} {
		if !strings.Contains(expr, attr) {
			t.Fatalf("progress remove expression %q missing %s", expr, attr)
		}
	}
}
