// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package audit

import (
	"context"

	"github.com/zeebo/errs"

	"storj.io/common/pb"
)

var (
	// ContainError is the containment errs class.
	ContainError = errs.Class("containment")

	// ErrContainedNotFound is the errs class for when a pending audit isn't found.
	ErrContainedNotFound = errs.Class("pending audit not found")

	// ErrContainDelete is the errs class for when a pending audit can't be deleted.
	ErrContainDelete = errs.Class("unable to delete pending audit")
)

// Containment holds information about pending audits for contained nodes.
//
// architecture: Database
type Containment interface {
	Get(ctx context.Context, nodeID pb.NodeID) (*ReverificationJob, error)
	Insert(ctx context.Context, job *PieceLocator) error
	Delete(ctx context.Context, job *PieceLocator) (wasDeleted, nodeStillContained bool, err error)
}
