package stats

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const testAddr = "localhost:6380"

func TestAggregateRead(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: testAddr})

	day := "2020-01-01"
	t.Cleanup(func() {
		client.Del(ctx,
			events(day, 1),
			activePubkeys(day),
			creatorPubkeys(day),
			stats(day),
		)
		client.Close()
	})

	db := NewDB(client, []int{1}) // kind 1 counts as a creator kind

	// Seed the HLL accumulators directly.
	client.PFAdd(ctx, events(day, 1), "event1", "event2", "event3")
	client.PFAdd(ctx, activePubkeys(day), "pk1", "pk2")
	client.PFAdd(ctx, creatorPubkeys(day), "pk1")

	if err := db.Aggregate(day); err != nil {
		t.Fatalf("Aggregate: %v", err)
	}

	result, err := db.Read(ctx, []string{day})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	s, ok := result[day]
	if !ok {
		t.Fatalf("no stats returned for day %s", day)
	}

	// HLL is approximate (~1% error), but counts this small are exact.
	if s.ActivePubkeys != 2 {
		t.Errorf("ActivePubkeys: want 2, got %d", s.ActivePubkeys)
	}
	if s.CreatorPubkeys != 1 {
		t.Errorf("CreatorPubkeys: want 1, got %d", s.CreatorPubkeys)
	}
	if s.EventsByKind[1] != 3 {
		t.Errorf("EventsByKind[1]: want 3, got %d", s.EventsByKind[1])
	}
}

func TestParseStats(t *testing.T) {
	tests := []struct {
		name    string
		fields  map[string]string
		want    Day
		wantErr bool
	}{
		{
			name: "all known fields",
			fields: map[string]string{
				KeyActivePubkeys:  "10",
				KeyCreatorPubkeys: "5",
				KeyTotalPubkeys:   "100",
			},
			want: Day{
				ActivePubkeys:  10,
				CreatorPubkeys: 5,
				TotalPubkeys:   100,
				EventsByKind:   map[int]int64{},
			},
		},
		{
			name: "kind fields",
			fields: map[string]string{
				"kind:1": "42",
				"kind:3": "7",
			},
			want: Day{
				EventsByKind: map[int]int64{1: 42, 3: 7},
			},
		},
		{
			name: "mixed known and kind fields",
			fields: map[string]string{
				KeyActivePubkeys: "3",
				"kind:1":         "9",
			},
			want: Day{
				ActivePubkeys: 3,
				EventsByKind:  map[int]int64{1: 9},
			},
		},
		{
			name:    "non-integer value returns error",
			fields:  map[string]string{KeyActivePubkeys: "not-a-number"},
			wantErr: true,
		},
		{
			name:    "kind field with non-integer value returns error",
			fields:  map[string]string{"kind:1": "abc"},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseStats(test.fields)
			if test.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("expected %v, got %v", test.want, got)
			}
		})
	}
}

func TestDailyRange(t *testing.T) {
	day := func(s string) time.Time {
		d, _ := time.Parse(DayFormat, s)
		return d
	}

	tests := []struct {
		name  string
		start time.Time
		end   time.Time
		want  []string
	}{
		{
			name:  "single day",
			start: day("2024-01-01"),
			end:   day("2024-01-02"),
			want:  []string{"2024-01-01"},
		},
		{
			name:  "multiple days",
			start: day("2024-01-01"),
			end:   day("2024-01-04"),
			want:  []string{"2024-01-01", "2024-01-02", "2024-01-03"},
		},
		{
			name:  "same start and end returns nil",
			start: day("2024-01-01"),
			end:   day("2024-01-01"),
			want:  nil,
		},
		{
			name:  "end before start returns nil",
			start: day("2024-01-05"),
			end:   day("2024-01-01"),
			want:  nil,
		},
		{
			name:  "non-midnight times are truncated",
			start: day("2024-03-01").Add(13*time.Hour + 45*time.Minute),
			end:   day("2024-03-03").Add(23*time.Hour + 59*time.Minute),
			want:  []string{"2024-03-01", "2024-03-02"},
		},
		{
			name:  "spans month boundary",
			start: day("2024-01-30"),
			end:   day("2024-02-02"),
			want:  []string{"2024-01-30", "2024-01-31", "2024-02-01"},
		},
		{
			name:  "spans year boundary",
			start: day("2023-12-31"),
			end:   day("2024-01-02"),
			want:  []string{"2023-12-31", "2024-01-01"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := DailyRange(test.start, test.end)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("want %v, got %v", test.want, got)
			}
		})
	}
}
