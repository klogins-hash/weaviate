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

package cluster

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
)

// Raft abstracts away the Raft store, providing clients with an interface that encompasses all query & write operations.
// It ensures that these operations are executed on the current leader, regardless of the specific leader in the cluster.
// If current node is the leader, then changes will be applied on the local node and bypass any networking requests.
type Raft struct {
	nodeSelector cluster.NodeSelector
	store        *Store
	cl           client
	log          *logrus.Logger
}

// client to communicate with remote services
type client interface {
	Apply(ctx context.Context, leaderAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error)
	Query(ctx context.Context, leaderAddr string, req *cmd.QueryRequest) (*cmd.QueryResponse, error)
	Remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error)
	Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
}

func NewRaft(selector cluster.NodeSelector, store *Store, client client) *Raft {
	return &Raft{nodeSelector: selector, store: store, cl: client, log: store.log}
}

// Open opens this store service and marked as such.
// It constructs a new Raft node using the provided configuration.
// If there is any old state, such as snapshots, logs, peers, etc., all of those will be restored
func (s *Raft) Open(ctx context.Context, db schema.Indexer) error {
	s.log.Info("starting raft sub-system ...")
	s.store.SetDB(db)
	return s.store.Open(ctx)
}

// Close() is called when the node is shutting down.
// Order of operations:

func (s *Raft) Close(ctx context.Context) (err error) {
	s.log.Info("shutting down raft sub-system ...")
	// IMMEDIATELY mark store as closed to reject any new operations
	s.store.open.Store(false)

	// non-voter can be safely removed, as they don't partake in RAFT elections
	if !s.store.IsVoter() {
		s.log.Info("removing this node from cluster prior to shutdown ...")
		if err := s.Remove(ctx, s.store.ID()); err != nil {
			s.log.WithError(err).Error("remove this node from cluster")
		} else {
			s.log.Info("successfully removed this node from the cluster.")
		}
	}

	// transfer leadership: it stops accepting client requests, ensures
	// the target server is up to date and initiates the transfer
	if s.store.IsLeader() {
		s.store.log.Info("transferring leadership to another server")
		if err := s.store.raft.LeadershipTransfer().Error(); err != nil {
			s.store.log.WithError(err).Error("transferring leadership")
		} else {
			s.log.Info("waiting for leadership transfer to complete...")
			timeout := time.After(10 * time.Second)
		transferLoop:
			for s.store.IsLeader() {
				select {
				case <-timeout:
					s.log.Warn("timeout waiting for leadership transfer")
					break transferLoop
				case <-ctx.Done():
					s.log.Warn("shutdown timeout during leadership transfer")
					break transferLoop
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}
			s.store.log.Info("leadership transfer completed")
		}
	}

	// Signal departure to peers FIRST - before shutting down Raft
	s.log.Info("leaving memberlist to signal departure to peers...")
	if err := s.nodeSelector.Leave(30 * time.Second); err != nil {
		s.store.log.WithError(err).Warn("leave memberlist")
	}

	// 3. NOW shutdown Raft to stop all consensus operations
	s.log.Info("stopping Raft operations after peers stopped sending traffic...")
	if err := s.store.raft.Shutdown().Error(); err != nil {
		s.store.log.WithError(err).Warn("shutdown raft")
	}

	s.log.Info("waiting for Raft operations to complete...")
	select {
	case <-ctx.Done():
		s.log.Warn("context cancelled during Raft operations wait")
	case <-time.After(3 * time.Second):
		s.log.Info("Raft operations wait completed")
	}

	s.log.Info("shutting down memberlist...")
	if err := s.nodeSelector.Shutdown(); err != nil {
		s.store.log.WithError(err).Warn("shutdown memberlist")
	}

	s.store.log.Info("closing raft-net ...")
	if err := s.store.raftTransport.Close(); err != nil {
		// it's not that fatal if we weren't able to close
		// the transport, that's why just warn
		s.store.log.WithError(err).Warn("close raft-net")
	}

	return s.store.Close(ctx)
}

func (s *Raft) Ready() bool {
	return s.store.Ready()
}

func (s *Raft) SchemaReader() schema.SchemaReader {
	return s.store.SchemaReader()
}

func (s *Raft) WaitUntilDBRestored(ctx context.Context, period time.Duration, close chan struct{}) error {
	return s.store.WaitToRestoreDB(ctx, period, close)
}

func (s *Raft) WaitForUpdate(ctx context.Context, schemaVersion uint64) error {
	return s.store.WaitForAppliedIndex(ctx, time.Millisecond*50, schemaVersion)
}

func (s *Raft) NodeSelector() cluster.NodeSelector {
	return s.nodeSelector
}

func (s *Raft) ReplicationFsm() *replication.ShardReplicationFSM {
	return s.store.replicationManager.GetReplicationFSM()
}
