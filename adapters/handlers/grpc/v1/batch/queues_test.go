//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch"
	"github.com/weaviate/weaviate/adapters/handlers/grpc/v1/batch/mocks"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
)

func TestHandler(t *testing.T) {
	ctx := context.Background()

	logger := logrus.New()

	t.Run("Send", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		// Arrange
		req := &pb.BatchSendRequest{
			StreamId: "test-stream",
			Message: &pb.BatchSendRequest_Objects{
				Objects: &pb.BatchObjects{
					Values: []*pb.BatchObject{{Collection: "TestClass"}},
				},
			},
		}

		shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
		defer shutdownCancel()

		writeQueues := batch.NewBatchWriteQueues()
		readQueues := batch.NewBatchReadQueues()
		internalQueue := batch.NewBatchInternalQueue()
		handler := batch.NewQueuesHandler(shutdownCtx, writeQueues, readQueues, logger)
		var wg sync.WaitGroup
		batch.StartScheduler(shutdownCtx, &wg, writeQueues, internalQueue, logger)

		writeQueues.Make(req.StreamId, nil)
		// Act
		howMany := handler.Send(ctx, req)

		// Assert
		require.Equal(t, int32(1), howMany, "Expected to send one object")

		// Verify that the internal queue has the object
		obj := <-internalQueue
		require.NotNil(t, obj, "Expected object to be sent to internal queue")

		// Shutdown the scheduler
		shutdownCancel()
		wg.Wait()
	})

	t.Run("Stream", func(t *testing.T) {
		t.Run("start and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			handler := batch.NewQueuesHandler(context.Background(), writeQueues, readQueues, logger)
			var wg sync.WaitGroup
			batch.StartScheduler(ctx, &wg, writeQueues, internalQueue, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			handler := batch.NewQueuesHandler(ctx, writeQueues, readQueues, logger)
			var wg sync.WaitGroup
			batch.StartScheduler(ctx, &wg, writeQueues, internalQueue, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewStopReadObject()
			}()

			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start and stop due to shutdown", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Shutdown{
					Shutdown: &pb.BatchShutdown{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
			handler := batch.NewQueuesHandler(shutdownCtx, writeQueues, readQueues, logger)
			var wg sync.WaitGroup
			batch.StartScheduler(shutdownCtx, &wg, writeQueues, internalQueue, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			shutdownCancel() // Trigger shutdown
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected no error when streaming")
		})

		t.Run("start, process error, and stop due to cancellation", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Error{
					Error: &pb.BatchError{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			handler := batch.NewQueuesHandler(context.Background(), writeQueues, readQueues, logger)
			var wg sync.WaitGroup
			batch.StartScheduler(ctx, &wg, writeQueues, internalQueue, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchError{{Error: "processing error"}})
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
		})

		t.Run("start, process error, and stop due to sentinel", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			stream := mocks.NewMockWeaviate_BatchStreamServer[pb.BatchStreamMessage](t)
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Start{
					Start: &pb.BatchStart{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Error{
					Error: &pb.BatchError{
						Error: "processing error",
					},
				},
			}).Return(nil).Once()
			stream.EXPECT().Send(&pb.BatchStreamMessage{
				Message: &pb.BatchStreamMessage_Stop{
					Stop: &pb.BatchStreamMessage_BatchStop{
						StreamId: StreamId,
					},
				},
			}).Return(nil).Once()

			writeQueues := batch.NewBatchWriteQueues()
			readQueues := batch.NewBatchReadQueues()
			internalQueue := batch.NewBatchInternalQueue()
			handler := batch.NewQueuesHandler(ctx, writeQueues, readQueues, logger)
			var wg sync.WaitGroup
			batch.StartScheduler(ctx, &wg, writeQueues, internalQueue, logger)

			writeQueues.Make(StreamId, nil)
			readQueues.Make(StreamId)
			ch, ok := readQueues.Get(StreamId)
			require.True(t, ok, "Expected read queue to exist")
			go func() {
				ch <- batch.NewErrorsObject([]*pb.BatchError{{Error: "processing error"}})
				ch <- batch.NewStopReadObject()
			}()

			readQueues.Make(StreamId)
			err := handler.Stream(ctx, StreamId, stream)
			require.NoError(t, err, "Expected error when processing")
		})
	})
}

func TestScheduler(t *testing.T) {
	ctx := context.Background()
	shutdownCtx, shutdownCancel := context.WithCancel(ctx)
	defer shutdownCancel()

	logger := logrus.New()
	writeQueues := batch.NewBatchWriteQueues()
	internalQueue := batch.NewBatchInternalQueue()

	writeQueues.Make("test-stream", nil)
	var wg sync.WaitGroup
	batch.StartScheduler(shutdownCtx, &wg, writeQueues, internalQueue, logger)

	queue, ok := writeQueues.Get("test-stream")
	require.True(t, ok, "Expected write queue to exist")

	obj := &pb.BatchObject{}
	queue <- batch.NewWriteObject(obj)

	require.Eventually(t, func() bool {
		select {
		case receivedObj := <-internalQueue:
			return receivedObj.Objects.Values[0] == obj
		default:
			return false
		}
	}, 1*time.Second, 10*time.Millisecond, "Expected object to be sent to internal queue")

	shutdownCancel() // Trigger shutdown
	wg.Wait()        // Wait for scheduler to finish

	require.Empty(t, internalQueue, "Expected internal queue to be empty after shutdown")
	ch, ok := writeQueues.Get("test-stream")
	require.True(t, ok, "Expected write queue to still exist after shutdown")
	require.Empty(t, ch, "Expected write queue to be empty after shutdown")
}
