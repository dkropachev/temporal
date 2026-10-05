package cassandra

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreallocatedResultCapacity(t *testing.T) {
	testCases := []struct {
		name string
		size int
		want int
	}{
		{
			name: "negative",
			size: -1,
			want: 0,
		},
		{
			name: "normal",
			size: 100,
			want: 100,
		},
		{
			name: "at limit",
			size: maxPreallocatedResultCapacity,
			want: maxPreallocatedResultCapacity,
		},
		{
			name: "above limit",
			size: maxPreallocatedResultCapacity + 1,
			want: maxPreallocatedResultCapacity,
		},
		{
			name: "maximum integer",
			size: math.MaxInt,
			want: maxPreallocatedResultCapacity,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, preallocatedResultCapacity(tc.size))
		})
	}
}
