package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestCalculateExternalPayloadSize_NoExternalPayloads(t *testing.T) {
	events := []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("test data"),
							},
						},
					},
				},
			},
		},
	}

	size, count, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
	require.Equal(t, int64(0), count)
}

func TestCalculateExternalPayloadSize_WithExternalPayloads(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	events := []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("reference"),
								ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
									{
										SizeBytes: 1024,
									},
									{
										SizeBytes: 2048,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					Result: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("result"),
								ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
									{
										SizeBytes: 512,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	size, count, err := CalculateExternalPayloadSize(events, metricsHandler)
	require.NoError(t, err)
	require.Equal(t, int64(1024+2048+512), size)
	require.Equal(t, int64(3), count)

	snapshot := capture.Snapshot()

	histogramRecs := snapshot[metrics.ExternalPayloadUploadSize.Name()]
	require.Len(t, histogramRecs, 3)
	require.Equal(t, int64(1024), histogramRecs[0].Value)
	require.Equal(t, int64(2048), histogramRecs[1].Value)
	require.Equal(t, int64(512), histogramRecs[2].Value)
}

func TestCalculateExternalPayloadSize_EmptyEvents(t *testing.T) {
	events := []*historypb.HistoryEvent{}

	size, count, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
	require.Equal(t, int64(0), count)
}

func TestCalculateExternalPayloadSize_FastPathExceptions(t *testing.T) {
	events := []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
			},
			UserMetadata: &sdkpb.UserMetadata{
				Summary: &commonpb.Payload{
					ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
						{SizeBytes: 256},
					},
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					LastFailure: &failurepb.Failure{
						EncodedAttributes: &commonpb.Payload{
							ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
								{SizeBytes: 512},
							},
						},
					},
				},
			},
		},
	}

	size, count, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	require.Equal(t, int64(768), size)
	require.Equal(t, int64(2), count)
}

func TestCalculateExternalPayloadSize_FastPathNoPayloads(t *testing.T) {
	events := []*historypb.HistoryEvent{
		nil,
		{},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{},
			},
		},
	}

	size, count, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
	require.Equal(t, int64(0), count)
}

func BenchmarkCalculateExternalPayloadSize(b *testing.B) {
	payloads := &commonpb.Payloads{
		Payloads: []*commonpb.Payload{
			{Data: make([]byte, 256)},
		},
	}
	events := []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: payloads,
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					Input: payloads,
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					Result: payloads,
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
					Result: payloads,
				},
			},
		},
	}

	b.ReportAllocs()
	for b.Loop() {
		_, _, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
		require.NoError(b, err)
	}
}
