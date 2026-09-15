package ddl

import "testing"

func TestNormalizeReplicatedMergeTree(t *testing.T) {
	in := "CREATE TABLE db.t (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', '{replica}') ORDER BY x"
	got, err := NormalizeCreateTable(in)
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeReplicatedReplacingMergeTree(t *testing.T) {
	in := "CREATE TABLE db.t (x UInt64, v UInt64) ENGINE = ReplicatedReplacingMergeTree('/p', 'r', v) ORDER BY x"
	got, err := NormalizeCreateTable(in)
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TABLE db.t (x UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY x"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestNormalizeStripsStoragePolicy(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{
			"CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 's3_tiered'",
			"CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x",
		},
		{
			"CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, storage_policy = 's3_tiered', min_bytes_for_wide_part = 0",
			"CREATE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0",
		},
	}
	for _, test := range tests {
		got, err := NormalizeCreateTable(test.in)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("got %q, want %q", got, test.want)
		}
	}
}

func TestForTable(t *testing.T) {
	in := "CREATE TABLE `old_db`.`old_table` (x UInt64) ENGINE = MergeTree ORDER BY x"
	got, err := ForTable(in, "new_db", "new_table")
	if err != nil {
		t.Fatal(err)
	}
	want := "CREATE TABLE `new_db`.`new_table` (x UInt64) ENGINE = MergeTree ORDER BY x"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestTableName(t *testing.T) {
	database, table, hasDatabase, err := TableName("CREATE TABLE `db``name`.`tab``le` (x UInt64) ENGINE = MergeTree ORDER BY x")
	if err != nil {
		t.Fatal(err)
	}
	if !hasDatabase || database != "db`name" || table != "tab`le" {
		t.Fatalf("database=%q table=%q hasDatabase=%t", database, table, hasDatabase)
	}
}

func TestTableNameWithoutDatabase(t *testing.T) {
	database, table, hasDatabase, err := TableName("CREATE TABLE events (x UInt64) ENGINE = MergeTree ORDER BY x")
	if err != nil {
		t.Fatal(err)
	}
	if hasDatabase || database != "" || table != "events" {
		t.Fatalf("database=%q table=%q hasDatabase=%t", database, table, hasDatabase)
	}
}
