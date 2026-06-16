package state

import (
	"context"
	"errors"
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
