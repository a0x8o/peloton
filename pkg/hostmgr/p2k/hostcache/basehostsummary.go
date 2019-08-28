// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hostcache

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/yarpc/yarpcerrors"

	pbhost "github.com/uber/peloton/.gen/peloton/api/v1alpha/host"
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	pbpod "github.com/uber/peloton/.gen/peloton/api/v1alpha/pod"
	hostmgr "github.com/uber/peloton/.gen/peloton/private/hostmgr/v1alpha"
	"github.com/uber/peloton/pkg/common/v1alpha/constraints"
	p2kscalar "github.com/uber/peloton/pkg/hostmgr/p2k/scalar"
	"github.com/uber/peloton/pkg/hostmgr/scalar"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
)

// makes sure baseHostSummary implements HostSummary
var _ HostSummary = &baseHostSummary{}

// HostStatus represents status (Ready/Placing/Reserved/Held) of the host in
// host cache
type HostStatus int

const (
	// ReadyHost represents a host ready to be used.
	ReadyHost HostStatus = iota + 1

	// PlacingHost represents a host being used by placement engine.
	PlacingHost

	// ReservedHost represents a host that is reserved for tasks.
	ReservedHost
)

const (
	// hostHeldHostStatusTimeout is a timeout for resetting.
	// HeldHost status back to ReadyHost status.
	// TODO: Make this configurable (T3312219).
	hostHeldStatusTimeout = 3 * time.Minute

	// emptyLeaseID is used when the host is in READY state.
	emptyLeaseID = ""
)

type HostSummary interface {
	// TryMatch atomically tries to match the current host with given
	// HostFilter, and lock the host if it does.
	TryMatch(filter *hostmgr.HostFilter) Match

	// CompleteLease verifies that the leaseID on this host is still valid.
	CompleteLease(leaseID string, newPodToResMap map[string]scalar.Resources) error

	// CasStatus sets the status to new value if current value is old, otherwise
	// returns error.
	CasStatus(old, new HostStatus) error

	// GetCapacity returns the capacity of the host.
	GetCapacity() scalar.Resources

	// GetAllocated returns the allocation of the host.
	GetAllocated() scalar.Resources

	// GetAvailable returns the available resources of the host.
	GetAvailable() scalar.Resources

	// SetCapacity sets the capacity of the host.
	SetCapacity(r scalar.Resources)

	// SetAvailable sets the available resource of the host.
	SetAvailable(r scalar.Resources)

	// GetVersion returns the version of the host.
	GetVersion() string

	// SetVersion sets the version of the host.
	SetVersion(v string)

	// GetHostname returns the hostname of the host.
	GetHostname() string

	// GetHostStatus returns the HostStatus of the host.
	GetHostStatus() HostStatus

	// GetHostLease creates and returns a host lease.
	GetHostLease() *hostmgr.HostLease

	// TerminateLease is called when terminating the lease on a host.
	TerminateLease(leaseID string) error

	// HandlePodEvent is called when a pod event occurs for a pod
	// that affects this host.
	HandlePodEvent(event *p2kscalar.PodEvent)

	// HoldForPod holds the host for the pod specified.
	// If an error is returned, hostsummary would guarantee that
	// the host is not held for the task.
	HoldForPod(id *peloton.PodID) error

	// ReleaseHoldForPod release the hold of host for the pod specified.
	ReleaseHoldForPod(id *peloton.PodID)

	// GetHeldPods returns a slice of pods that puts the host in held.
	GetHeldPods() []*peloton.PodID

	// DeleteExpiredHolds deletes expired held pods in a hostSummary, returns
	// whether the hostSummary is free of helds,
	// available resource,
	// and the pods held expired.
	DeleteExpiredHolds(now time.Time) (bool, scalar.Resources, []*peloton.PodID)
}

// hostStrategy defines methods that shared by mesos/k8s hosts, but have different
// implementation.
// methods in the interface assumes lock is taken
type hostStrategy interface {
	// postCompleteLease handles actions after lease is completed
	postCompleteLease(newPodToResMap map[string]scalar.Resources) error
}

// baseHostSummary is a data struct holding resources and metadata of a host.
type baseHostSummary struct {
	mu sync.RWMutex

	// hostname of the host
	hostname string

	// labels on this host
	labels []*peloton.Label

	// a map of podIDs for which the host is held
	// key is the podID, value is the expiration time of the hold
	heldPodIDs map[string]time.Time

	// locking status of this host
	status HostStatus

	// LeaseID is a valid UUID when the host is locked for placement and will
	// be used to ensure that the the host is used to launch only those pods
	// for which the lease was acquired by placement engine. Will be empty if
	// host is not in placing state. This leaseID does not correspond to a
	// chunk of resources on that host, but the entire host. So we run the risk
	// of locking the entire host even if the resource constraint is small. We
	// can optimize this further by maintaining a list of leaseIDs per host.
	leaseID string

	// Resource version of this host.
	version string

	// strategy pattern adopted by the particular host
	strategy hostStrategy

	// capacity of the host
	capacity scalar.Resources

	// resources allocated on the host. this should always be equal to the sum
	// of resources in podToResMap
	allocated scalar.Resources

	// available resources on the host
	available scalar.Resources
}

// newBaseHostSummary returns a zero initialized HostSummary object.
func newBaseHostSummary(
	hostname string,
	version string,
) *baseHostSummary {
	return &baseHostSummary{
		status:     ReadyHost,
		hostname:   hostname,
		heldPodIDs: make(map[string]time.Time),
		version:    version,
		strategy:   &noopHostStrategy{},
	}
}

// TryMatch atomically tries to match the current host with given HostFilter,
// and lock the host if it does. If current baseHostSummary is matched, this host
// will be marked as `PLACING`, after which it cannot be used by another
// placement engine until released. If current host is not matched by given
// HostFilter, the host status will remain unchanged.
func (a *baseHostSummary) TryMatch(
	filter *hostmgr.HostFilter,
) Match {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status != ReadyHost {
		return Match{
			Result: hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
		}
	}

	// For a host held pods, we anticipate in place upgrades to happen. So, it
	// is only a match when the hint contains the host and we temporarily
	// reject any additional pod placements on the host.
	if a.isHeld() {
		var hintFound bool
		for _, hostHint := range filter.GetHint().GetHostHint() {
			if hostHint.GetHostname() == a.hostname {
				hintFound = true
				break
			}
		}

		if !hintFound {
			return Match{
				Result: hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
			}
		}
	}

	result := a.matchHostFilter(filter)

	if result != hostmgr.HostFilterResult_HOST_FILTER_MATCH {
		return Match{Result: result}
	}

	// TODO: Handle oversubscription

	// Setting status to `PlacingHost`: this ensures proper state tracking of
	// resources on the host and also ensures that this host will not be used by
	// another placement engine before it is released.
	err := a.casStatus(a.status, PlacingHost)
	if err != nil {
		return Match{
			Result: hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_STATUS,
		}
	}

	return Match{
		Result:   hostmgr.HostFilterResult_HOST_FILTER_MATCH,
		HostName: a.hostname,
	}
}

// CompleteLease verifies that the leaseID on this host is still valid.
// It checks that current baseHostSummary is in Placing status, updates podToResMap
// to the host summary, recalculates allocated resources and set the host status
// to Ready.
func (a *baseHostSummary) CompleteLease(
	leaseID string,
	newPodToResMap map[string]scalar.Resources,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status != PlacingHost {
		return yarpcerrors.InvalidArgumentErrorf("host status is not Placing")
	}

	if a.leaseID != leaseID {
		return yarpcerrors.InvalidArgumentErrorf("host leaseID does not match")
	}

	if err := a.casStatus(PlacingHost, ReadyHost); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("failed to unlock host: %s", err)
	}

	if err := a.strategy.postCompleteLease(newPodToResMap); err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"hostname": a.hostname,
		"pods":     newPodToResMap,
	}).Debug("pods added to the host for launch")

	return nil
}

// CasStatus sets the status to new value if current value is old, otherwise
// returns error.
func (a *baseHostSummary) CasStatus(old, new HostStatus) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.casStatus(old, new); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("failed to set cas status: %s", err)
	}

	return nil
}

// GetVersion returns the version of the host.
func (a *baseHostSummary) GetVersion() string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.version
}

// SetVersion sets the version of the host.
func (a *baseHostSummary) SetVersion(v string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.version = v
}

// GetHostname returns the hostname of the host.
func (a *baseHostSummary) GetHostname() string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.hostname
}

// GetHostStatus returns the HostStatus of the host.
func (a *baseHostSummary) GetHostStatus() HostStatus {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.status
}

// GetHostLease creates and returns a host lease.
func (a *baseHostSummary) GetHostLease() *hostmgr.HostLease {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return &hostmgr.HostLease{
		LeaseId: &hostmgr.LeaseID{
			Value: a.leaseID,
		},
		HostSummary: &pbhost.HostSummary{
			Hostname:  a.hostname,
			Resources: scalar.ToPelotonResources(a.available),
			Labels:    a.labels,
		},
	}
}

// TerminateLease is called when terminating the lease on a host.
// This will be called when host in PLACING state is not used, and placement
// engine decides to terminate its lease and set the host back to Ready
func (a *baseHostSummary) TerminateLease(leaseID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status != PlacingHost {
		return yarpcerrors.InvalidArgumentErrorf("invalid status %v", a.status)
	}

	// TODO: lease may be expired already.
	if a.leaseID != leaseID {
		return yarpcerrors.InvalidArgumentErrorf("host leaseID does not match")
	}

	if err := a.casStatus(PlacingHost, ReadyHost); err != nil {
		return yarpcerrors.InvalidArgumentErrorf("failed to set cas status: %s", err)
	}

	return nil
}

// GetCapacity returns the capacity of the host.
func (a *baseHostSummary) GetCapacity() scalar.Resources {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.capacity
}

// GetAllocated returns the allocation of the host.
func (a *baseHostSummary) GetAllocated() scalar.Resources {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.allocated
}

// HoldForPod adds pod to heldPodIDs map when host is not reserved. It is noop
// if pod ready exists in the map.
func (a *baseHostSummary) HoldForPod(id *peloton.PodID) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.status == ReservedHost {
		return yarpcerrors.InvalidArgumentErrorf("invalid status %v for holding", a.status)
	}

	if _, ok := a.heldPodIDs[id.GetValue()]; !ok {
		a.heldPodIDs[id.GetValue()] = time.Now().Add(hostHeldStatusTimeout)
	}

	log.WithFields(log.Fields{
		"hostname":  a.hostname,
		"pods_held": a.heldPodIDs,
		"pod_id":    id.GetValue(),
	}).Debug("Hold for pod")
	return nil
}

// ReleaseHoldForPod removes the pod from heldPodIDs map. It should be called
// when:
// 1. pod is upgraded in place.
// 2. hold for this pod expires.
func (a *baseHostSummary) ReleaseHoldForPod(id *peloton.PodID) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.releaseHoldForPod(id)
	return
}

// GetHeldPods returns a list of held PodIDs.
func (a *baseHostSummary) GetHeldPods() []*peloton.PodID {
	a.mu.Lock()
	defer a.mu.Unlock()

	var result []*peloton.PodID
	for id := range a.heldPodIDs {
		result = append(result, &peloton.PodID{Value: id})
	}
	return result
}

// DeleteExpiredHolds deletes expired held pods in a hostSummary, returns
// whether the hostSummary is free of helds,
// available resource,
// and the pods held expired.
func (a *baseHostSummary) DeleteExpiredHolds(
	deadline time.Time) (bool, scalar.Resources, []*peloton.PodID) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var expired []*peloton.PodID
	for id, expirationTime := range a.heldPodIDs {
		if deadline.After(expirationTime) {
			pod := &peloton.PodID{Value: id}
			a.releaseHoldForPod(pod)
			expired = append(expired, pod)
		}
	}
	return !a.isHeld(), a.available, expired
}

func (a *baseHostSummary) releaseHoldForPod(id *peloton.PodID) {
	if _, ok := a.heldPodIDs[id.GetValue()]; !ok {
		// This can happen for various reasons such as a task is launched again
		// on the same host after timeout.
		log.WithFields(log.Fields{
			"hostname": a.hostname,
			"pod_id":   id.GetValue(),
		}).Info("Host not held for pod")
		return
	}

	delete(a.heldPodIDs, id.GetValue())

	log.WithFields(log.Fields{
		"hostname": a.hostname,
		"pod_id":   id.GetValue(),
	}).Debug("Release hold for pod")
}

// isHeld is true when number of held PodIDs is greater than zero.
func (a *baseHostSummary) isHeld() bool {
	return len(a.heldPodIDs) > 0
}

// GetAvailable returns the available resources of the host.
func (a *baseHostSummary) GetAvailable() scalar.Resources {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.available
}

// HandlePodEvent is a noop for baseHostSummary, corresponding subclasses should
// overwrite the method
func (a *baseHostSummary) HandlePodEvent(event *p2kscalar.PodEvent) {
	return
}

// SetCapacity sets the capacity of the host.
func (a *baseHostSummary) SetCapacity(r scalar.Resources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.capacity = r
}

// SetAvailable sets the available resources of the host
func (a *baseHostSummary) SetAvailable(r scalar.Resources) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.available = r
}

// casStatus lock-freely sets the status to new value and update lease ID if
// current value is old, otherwise returns error.
// This function assumes baseHostSummary lock is held before calling.
func (a *baseHostSummary) casStatus(oldStatus, newStatus HostStatus) error {
	if a.status != oldStatus {
		return fmt.Errorf("invalid old status: %v", oldStatus)
	}
	a.status = newStatus

	switch a.status {
	case ReadyHost:
		// if its a ready host then reset the hostOfferID
		a.leaseID = emptyLeaseID
	case PlacingHost:
		// generate the offer id for a placing host.
		a.leaseID = uuid.New()
	case ReservedHost:
		// generate the offer id for a placing host.
		a.leaseID = uuid.New()
	}
	return nil
}

// matchHostFilter determines whether given HostFilter matches the host.
// This function assumes baseHostSummary lock is held before calling.
func (a *baseHostSummary) matchHostFilter(
	c *hostmgr.HostFilter,
) hostmgr.HostFilterResult {

	min := c.GetResourceConstraint().GetMinimum()
	if min != nil {
		// Get min required resources.
		minRes := scalar.FromResourceSpec(min)
		if !a.available.Contains(minRes) {
			return hostmgr.HostFilterResult_HOST_FILTER_INSUFFICIENT_RESOURCES
		}
	}

	// TODO: Match ports resources.

	sc := c.GetSchedulingConstraint()

	// If constraints don't specify an exclusive host, then reject
	// hosts that are designated as exclusive.
	if constraints.IsNonExclusiveConstraint(sc) &&
		constraints.HasExclusiveLabel(a.labels) {
		log.WithField("hostname", a.hostname).Debug("Skipped exclusive host")
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_CONSTRAINTS
	}

	if sc == nil {
		// No scheduling constraint, we have a match.
		return hostmgr.HostFilterResult_HOST_FILTER_MATCH
	}

	// Only evaluator based on host constraints is in use.
	evaluator := constraints.NewEvaluator(
		pbpod.LabelConstraint_LABEL_CONSTRAINT_KIND_HOST)

	lv := constraints.GetHostLabelValues(a.hostname, a.labels)
	result, err := evaluator.Evaluate(sc, lv)
	if err != nil {
		log.WithError(err).
			Error("Evaluating input constraint")
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_CONSTRAINTS
	}

	switch result {
	case constraints.EvaluateResultMatch:
		fallthrough
	case constraints.EvaluateResultNotApplicable:
		log.WithFields(log.Fields{
			"labels":     lv,
			"hostname":   a.hostname,
			"constraint": sc,
		}).Debug("Attributes match constraint")
	default:
		log.WithFields(log.Fields{
			"labels":     lv,
			"hostname":   a.hostname,
			"constraint": sc,
		}).Debug("Attributes do not match constraint")
		return hostmgr.HostFilterResult_HOST_FILTER_MISMATCH_CONSTRAINTS
	}

	return hostmgr.HostFilterResult_HOST_FILTER_MATCH
}

type noopHostStrategy struct{}

func (s *noopHostStrategy) postCompleteLease(newPodToResMap map[string]scalar.Resources) error {
	return nil
}
