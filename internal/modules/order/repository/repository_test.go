package repository

import (
	"reflect"
	"testing"
)

func TestUserIDAliasesForDBMatchesNumericAndPrefixedRows(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want []string
	}{
		{name: "numeric", id: "1001", want: []string{"1001", "u_1001"}},
		{name: "prefixed", id: "u_1001", want: []string{"1001", "u_1001"}},
		{name: "upper_prefixed", id: "U_1001", want: []string{"1001", "u_1001", "U_1001"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := userIDAliasesForDB(tt.id); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("userIDAliasesForDB(%q) = %+v, want %+v", tt.id, got, tt.want)
			}
		})
	}
}
