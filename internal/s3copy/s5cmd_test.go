package s3copy

import (
	"reflect"
	"testing"
)

func TestArgsIncludesNumWorkers(t *testing.T) {
	copier := Copier{Endpoint: "http://localhost:4566", NumWorkers: 64}
	got := copier.args("cp", "/tmp/source/", "s3://bucket/prefix/")
	want := []string{
		"--retry-count", "0",
		"--numworkers", "64",
		"--endpoint-url", "http://localhost:4566",
		"cp",
		"/tmp/source/",
		"s3://bucket/prefix/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}

func TestArgsOmitsNumWorkersWhenUnset(t *testing.T) {
	copier := Copier{}
	got := copier.args("cp", "/tmp/source/", "s3://bucket/prefix/")
	want := []string{
		"--retry-count", "0",
		"cp",
		"/tmp/source/",
		"s3://bucket/prefix/",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("args = %#v, want %#v", got, want)
	}
}
