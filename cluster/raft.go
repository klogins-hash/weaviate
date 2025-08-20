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
	"fmt"
	"time"

	"github.com/hashicorp/raft"
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
func (s *Raft) Close(ctx context.Context) (err error) {
	s.log.Info("shutting down raft sub-system ...")

	// Set store as closed immediately to signal not ready
	s.store.open.Store(false)
	s.log.Info("marked store as closed - node no longer ready")

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

			// Verify that a new leader has been elected
			s.log.Info("verifying new leader election...")
			if err := s.waitForNewLeader(ctx); err != nil {
				s.log.WithError(err).Warn("failed to verify new leader, proceeding anyway")
			} else {
				s.log.Info("confirmed: new leader has been elected")
			}
		}
	}

	// Remove from Raft configuration after leadership transfer (for all nodes)
	s.log.Info("removing this node from Raft configuration...")
	if err := s.store.raft.RemoveServer(raft.ServerID(s.store.ID()), 0, 0).Error(); err != nil {
		s.log.WithError(err).Warn("remove from Raft configuration")
	} else {
		s.log.Info("successfully removed from Raft configuration")
	}

	// wait for the configuration change to be applied
	time.Sleep(3 * time.Second)

	s.log.Info("leaving memberlist ...")
	if err := s.nodeSelector.Leave(30 * time.Second); err != nil {
		s.store.log.WithError(err).Warn("leave memberlist")
	}

	// Wait a bit for gossip to propagate before closing transport
	s.log.Info("waiting for gossip propagation and drain mode...")
	time.Sleep(3 * time.Second)

	// Close transport after gossip propagation to prevent Raft traffic
	s.store.log.Info("closing raft-net after gossip propagation...")
	if err := s.store.raftTransport.Close(); err != nil {
		s.store.log.WithError(err).Warn("close raft-net")
	}

	s.log.Info("shutting down memberlist...")
	if err := s.nodeSelector.Shutdown(); err != nil {
		s.store.log.WithError(err).Warn("shutdown memberlist")
	}

	s.log.Info("stopping raft operations ...")
	if err := s.store.raft.Shutdown().Error(); err != nil {
		s.store.log.WithError(err).Warn("shutdown raft")
	}

	return s.store.Close(ctx)
}

// waitForNewLeader waits for a new leader to be elected after leadership transfer
func (s *Raft) waitForNewLeader(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for new leader: %w", ctx.Err())
		case <-ticker.C:
			newLeader := s.store.Leader()
			if newLeader != "" && newLeader != s.store.ID() {
				s.log.WithField("new_leader", newLeader).Info("new leader confirmed")
				return nil
			}
		}
	}
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
