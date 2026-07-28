package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/tasks"
)

const benchmarkExecutableTrackerTaskCount = 1024

var benchmarkExecutableTrackerSink *executableTracker

type benchmarkTrackerExecutable struct {
	Executable

	key         tasks.Key
	namespaceID string
}

func (e *benchmarkTrackerExecutable) GetKey() tasks.Key {
	return e.key
}

func (e *benchmarkTrackerExecutable) GetNamespaceID() string {
	return e.namespaceID
}

func TestExecutableTrackerSplit(t *testing.T) {
	testCases := []struct {
		name              string
		splitTaskID       int64
		expectedLeftSize  int
		expectedRightSize int
		expectedLeftCount int
	}{
		{
			name:              "no tasks moved",
			splitTaskID:       4,
			expectedLeftSize:  4,
			expectedRightSize: 0,
			expectedLeftCount: 4,
		},
		{
			name:              "half tasks moved",
			splitTaskID:       2,
			expectedLeftSize:  2,
			expectedRightSize: 2,
			expectedLeftCount: 2,
		},
		{
			name:              "all tasks moved",
			splitTaskID:       0,
			expectedLeftSize:  0,
			expectedRightSize: 4,
			expectedLeftCount: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fullScope := NewScope(
				NewRange(tasks.NewImmediateKey(0), tasks.NewImmediateKey(4)),
				predicates.Universal[tasks.Task](),
			)
			leftScope, rightScope := fullScope.SplitByRange(tasks.NewImmediateKey(testCase.splitTaskID))
			tracker := newExecutableTrackerForTest(4)

			left, right := tracker.split(leftScope, rightScope)

			require.NotNil(t, left.pendingExecutables)
			require.NotNil(t, left.pendingPerKey)
			require.NotNil(t, right.pendingExecutables)
			require.NotNil(t, right.pendingPerKey)
			require.Len(t, left.pendingExecutables, testCase.expectedLeftSize)
			require.Len(t, right.pendingExecutables, testCase.expectedRightSize)
			require.Equal(t, testCase.expectedLeftCount, left.pendingPerKey["namespace"])
			require.Equal(t, 4-testCase.expectedLeftCount, right.pendingPerKey["namespace"])
			for _, executable := range left.pendingExecutables {
				require.True(t, leftScope.Contains(executable))
			}
			for _, executable := range right.pendingExecutables {
				require.True(t, rightScope.Contains(executable))
			}
		})
	}
}

func BenchmarkExecutableTrackerSplit(b *testing.B) {
	fullScope := NewScope(
		NewRange(tasks.NewImmediateKey(0), tasks.NewImmediateKey(benchmarkExecutableTrackerTaskCount)),
		predicates.Universal[tasks.Task](),
	)

	b.Run("no_tasks_moved", func(b *testing.B) {
		emptyScope := NewScope(
			NewRange(
				tasks.NewImmediateKey(benchmarkExecutableTrackerTaskCount),
				tasks.NewImmediateKey(benchmarkExecutableTrackerTaskCount),
			),
			predicates.Universal[tasks.Task](),
		)
		tracker := newExecutableTrackerForTest(benchmarkExecutableTrackerTaskCount)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			tracker, benchmarkExecutableTrackerSink = tracker.split(fullScope, emptyScope)
		}
	})

	b.Run("half_tasks_moved", func(b *testing.B) {
		leftScope, rightScope := fullScope.SplitByRange(
			tasks.NewImmediateKey(benchmarkExecutableTrackerTaskCount / 2),
		)
		tracker := newExecutableTrackerForTest(benchmarkExecutableTrackerTaskCount)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			var right *executableTracker
			tracker, right = tracker.split(leftScope, rightScope)
			benchmarkExecutableTrackerSink = right
			tracker = tracker.merge(right)
		}
	})

	b.Run("all_tasks_moved", func(b *testing.B) {
		emptyScope := NewScope(
			NewRange(tasks.NewImmediateKey(0), tasks.NewImmediateKey(0)),
			predicates.Universal[tasks.Task](),
		)
		tracker := newExecutableTrackerForTest(benchmarkExecutableTrackerTaskCount)

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			var right *executableTracker
			tracker, right = tracker.split(emptyScope, fullScope)
			benchmarkExecutableTrackerSink = right
			tracker = tracker.merge(right)
		}
	})
}

func newExecutableTrackerForTest(taskCount int) *executableTracker {
	tracker := newExecutableTracker(GrouperNamespaceID{})
	for taskID := 0; taskID < taskCount; taskID++ {
		tracker.add(&benchmarkTrackerExecutable{
			key:         tasks.NewImmediateKey(int64(taskID)),
			namespaceID: "namespace",
		})
	}
	return tracker
}
