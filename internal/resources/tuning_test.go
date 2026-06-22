package resources

import (
	"strconv"
	"testing"
)

func TestInsertSelectSettings(t *testing.T) {
	settings, err := InsertSelectSettings(Limits{CPUs: 6, MemoryBytes: 10_000})
	if err != nil {
		t.Fatal(err)
	}
	if settings["max_threads"] != "3" {
		t.Fatalf("max_threads = %q", settings["max_threads"])
	}
	if settings["max_insert_threads"] != "3" {
		t.Fatalf("max_insert_threads = %q", settings["max_insert_threads"])
	}
	if settings["max_memory_usage"] != "8000" {
		t.Fatalf("max_memory_usage = %q", settings["max_memory_usage"])
	}
	if settings["min_insert_block_size_rows"] != "0" {
		t.Fatalf("min_insert_block_size_rows = %q", settings["min_insert_block_size_rows"])
	}
	if settings["min_insert_block_size_bytes"] != "888" {
		t.Fatalf("min_insert_block_size_bytes = %q", settings["min_insert_block_size_bytes"])
	}
}

func TestInsertThreadCount(t *testing.T) {
	tests := []struct {
		cpus int
		want int
	}{
		{cpus: 1, want: 1},
		{cpus: 2, want: 1},
		{cpus: 3, want: 1},
		{cpus: 8, want: 4},
		{cpus: 16, want: 8},
	}

	for _, tt := range tests {
		if got := insertThreadCount(tt.cpus); got != tt.want {
			t.Fatalf("insertThreadCount(%d) = %d, want %d", tt.cpus, got, tt.want)
		}
	}
}

func TestInsertSelectSettingsKeepBlockMemoryWithinBudget(t *testing.T) {
	tests := []struct {
		name        string
		limits      Limits
		wantThreads uint64
	}{
		{
			name:        "moderate worker",
			limits:      Limits{CPUs: 8, MemoryBytes: 32 * 1024 * 1024 * 1024},
			wantThreads: 4,
		},
		{
			name:        "large worker",
			limits:      Limits{CPUs: 16, MemoryBytes: 64 * 1024 * 1024 * 1024},
			wantThreads: 8,
		},
		{
			name:        "high cpu worker",
			limits:      Limits{CPUs: 64, MemoryBytes: 256 * 1024 * 1024 * 1024},
			wantThreads: 32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, err := InsertSelectSettings(tt.limits)
			if err != nil {
				t.Fatal(err)
			}
			threads := mustParseUintSetting(t, settings["max_insert_threads"])
			if threads != tt.wantThreads {
				t.Fatalf("max_insert_threads = %d, want %d", threads, tt.wantThreads)
			}
			if got := settings["max_threads"]; got != settings["max_insert_threads"] {
				t.Fatalf("max_threads = %q, want %q", got, settings["max_insert_threads"])
			}
			if settings["min_insert_block_size_rows"] != "0" {
				t.Fatalf("min_insert_block_size_rows = %q, want 0", settings["min_insert_block_size_rows"])
			}

			maxMemoryUsage := mustParseUintSetting(t, settings["max_memory_usage"])
			minBlockBytes := mustParseUintSetting(t, settings["min_insert_block_size_bytes"])
			wantMaxMemoryUsage := tt.limits.MemoryBytes * insertMemoryUsagePercent / 100
			if maxMemoryUsage != wantMaxMemoryUsage {
				t.Fatalf("max_memory_usage = %d, want %d", maxMemoryUsage, wantMaxMemoryUsage)
			}

			if got, want := minBlockBytes, maxMemoryUsage/(insertBlockMemoryDivisor*threads); got != want {
				t.Fatalf("min_insert_block_size_bytes = %d, want %d", got, want)
			}
			if reserved := minBlockBytes * threads * insertBlockMemoryDivisor; reserved > maxMemoryUsage {
				t.Fatalf("block memory budget = %d, exceeds max_memory_usage %d", reserved, maxMemoryUsage)
			}
		})
	}
}

func TestInsertSelectSettingsReducedThreadsIncreaseBlockSize(t *testing.T) {
	limits := Limits{CPUs: 16, MemoryBytes: 64 * 1024 * 1024 * 1024}
	settings, err := InsertSelectSettings(limits)
	if err != nil {
		t.Fatal(err)
	}

	maxMemoryUsage := mustParseUintSetting(t, settings["max_memory_usage"])
	minBlockBytes := mustParseUintSetting(t, settings["min_insert_block_size_bytes"])
	fullCPUThreadBlockBytes := maxMemoryUsage / (insertBlockMemoryDivisor * uint64(limits.CPUs))
	if minBlockBytes != fullCPUThreadBlockBytes*2 {
		t.Fatalf("min_insert_block_size_bytes = %d, want double full-cpu block size %d", minBlockBytes, fullCPUThreadBlockBytes*2)
	}
}

func TestMergeBackgroundPoolSize(t *testing.T) {
	tests := []struct {
		name   string
		limits Limits
		want   int
	}{
		{
			name:   "small cpu count satisfies ClickHouse merge tree defaults",
			limits: Limits{CPUs: 4},
			want:   13,
		},
		{
			name:   "larger cpu count uses detected cpu count",
			limits: Limits{CPUs: 16},
			want:   16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MergeBackgroundPoolSize(tt.limits)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("MergeBackgroundPoolSize(%+v) = %d, want %d", tt.limits, got, tt.want)
			}
		})
	}
}

func mustParseUintSetting(t *testing.T, raw string) uint64 {
	t.Helper()
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		t.Fatalf("parse setting %q: %v", raw, err)
	}
	return value
}

func TestMergeBackgroundPoolSizeRejectsInvalidCPUCount(t *testing.T) {
	if _, err := MergeBackgroundPoolSize(Limits{}); err == nil {
		t.Fatal("expected invalid cpu count error")
	}
}

func TestMergeTreeSettingsForLimits(t *testing.T) {
	tests := []struct {
		name      string
		limits    Limits
		wantRows  uint64
		wantBytes uint64
	}{
		{
			name:      "low memory clamps to safe minimum",
			limits:    Limits{CPUs: 8, MemoryBytes: 1 * 1024 * 1024 * 1024},
			wantRows:  8192,
			wantBytes: 9 * 1024 * 1024,
		},
		{
			name:      "scales with memory per background worker",
			limits:    Limits{CPUs: 16, MemoryBytes: 32 * 1024 * 1024 * 1024},
			wantRows:  155648,
			wantBytes: 153 * 1024 * 1024,
		},
		{
			name:      "high memory clamps to upper bound",
			limits:    Limits{CPUs: 16, MemoryBytes: 1024 * 1024 * 1024 * 1024},
			wantRows:  262144,
			wantBytes: 256 * 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings, err := MergeTreeSettingsForLimits(tt.limits)
			if err != nil {
				t.Fatal(err)
			}
			if settings.MergeMaxBlockSize != tt.wantRows {
				t.Fatalf("merge_max_block_size = %d, want %d", settings.MergeMaxBlockSize, tt.wantRows)
			}
			if settings.MergeMaxBlockSizeBytes != tt.wantBytes {
				t.Fatalf("merge_max_block_size_bytes = %d, want %d", settings.MergeMaxBlockSizeBytes, tt.wantBytes)
			}
			if settings.MergeSelectingSleepMS != defaultMergeSelectingSleepMS {
				t.Fatalf("merge_selecting_sleep_ms = %d, want %d", settings.MergeSelectingSleepMS, defaultMergeSelectingSleepMS)
			}
			if settings.MergeSchedulingPolicy != defaultMergeSchedulingPolicy {
				t.Fatalf("background_merges_mutations_scheduling_policy = %q, want %q", settings.MergeSchedulingPolicy, defaultMergeSchedulingPolicy)
			}
			if settings.DefaultCompressionCodec != DefaultCompressionCodec {
				t.Fatalf("default_compression_codec = %q, want %q", settings.DefaultCompressionCodec, DefaultCompressionCodec)
			}
		})
	}
}

func TestParseCgroupV2CPUQuota(t *testing.T) {
	cpus, limited, err := parseCgroupV2CPUQuota("250000 100000")
	if err != nil {
		t.Fatal(err)
	}
	if !limited || cpus != 3 {
		t.Fatalf("limited = %v, cpus = %d", limited, cpus)
	}

	_, limited, err = parseCgroupV2CPUQuota("max 100000")
	if err != nil {
		t.Fatal(err)
	}
	if limited {
		t.Fatal("expected unlimited cpu.max")
	}
}

func TestParseCPUSet(t *testing.T) {
	count, err := parseCPUSet("0-3,6,8-9")
	if err != nil {
		t.Fatal(err)
	}
	if count != 7 {
		t.Fatalf("count = %d", count)
	}
}

func TestParseCgroupMemoryLimit(t *testing.T) {
	memory, limited, err := parseCgroupMemoryLimit("12345")
	if err != nil {
		t.Fatal(err)
	}
	if !limited || memory != 12345 {
		t.Fatalf("limited = %v, memory = %d", limited, memory)
	}

	_, limited, err = parseCgroupMemoryLimit("max")
	if err != nil {
		t.Fatal(err)
	}
	if limited {
		t.Fatal("expected unlimited memory.max")
	}
}
