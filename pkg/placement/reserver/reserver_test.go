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

package reserver

import (
	"context"
	"errors"
	"testing"

	"github.com/uber/peloton/.gen/mesos/v1"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"
	"github.com/uber/peloton/.gen/peloton/private/resmgr"

	queue_mocks "github.com/uber/peloton/pkg/common/queue/mocks"
	"github.com/uber/peloton/pkg/placement/config"
	hosts_mock "github.com/uber/peloton/pkg/placement/hosts/mocks"
	"github.com/uber/peloton/pkg/placement/metrics"
	"github.com/uber/peloton/pkg/placement/models"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
)

type ReserverTestSuite struct {
	suite.Suite
	mockCtrl    *gomock.Controller
	hostService *hosts_mock.MockService
	reserver    Reserver
}

func (suite *ReserverTestSuite) SetupTest() {
	// Setting up the test by that each test will have
	// its own Reserver and host service
	suite.mockCtrl = gomock.NewController(suite.T())
	defer suite.mockCtrl.Finish()
	metrics := metrics.NewMetrics(tally.NoopScope)

	suite.hostService = hosts_mock.NewMockService(suite.mockCtrl)
	config := &config.PlacementConfig{}

	suite.reserver = NewReserver(
		metrics,
		config,
		suite.hostService,
	)
}

func TestReserver(t *testing.T) {
	suite.Run(t, new(ReserverTestSuite))
}

func (suite *ReserverTestSuite) TestReserverStart() {
	suite.reserver.Start()
}

func (suite *ReserverTestSuite) TestReserverStop() {
	suite.reserver.Stop()
}

// TestReservation tries to test the reservation is working as expected
func (suite *ReserverTestSuite) TestReservation() {
	task := createResMgrTask()
	hosts := []*models.Host{
		{
			Host: creareHostInfo(),
		},
	}

	// Calling mockups for the host service which reserver will call
	suite.hostService.EXPECT().GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts, nil)
	suite.hostService.EXPECT().ReserveHost(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	// Adding task to reservation queue which is simulating the behavior
	// for placement engine got the task from Resource Manager
	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), float64(0))
	suite.NoError(err)
}

// TestReservationNoTasks tests if there is no task in the queue
// to reserve. We are simulating that by not adding any task in
// reservation queue.
func (suite *ReserverTestSuite) TestReservationNoTasks() {
	suite.hostService.EXPECT().GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noTasksTimeoutPenalty.Seconds())
	require.Error(suite.T(), err)
	suite.Contains(err.Error(), "No items in reservation queue")
}

// TestReservationInvalidTask making the resmgr task invalid
// by making id nil, which simulate error condition task is not valid
func (suite *ReserverTestSuite) TestReservationInvalidTask() {
	task := createResMgrTask()
	task.Id = nil
	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noTasksTimeoutPenalty.Seconds())
	require.Error(suite.T(), err)
	suite.Contains(err.Error(), "Not a valid task")
}

// TestReservationErrorinAcquire simulates the error in Acquire call
// from the HostService and verify that.
func (suite *ReserverTestSuite) TestReservationErrorinAcquire() {
	task := createResMgrTask()
	suite.hostService.EXPECT().
		GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("error in acquire hosts"))

	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noHostsTimeoutPenalty.Seconds())
	require.Error(suite.T(), err)
	suite.Contains(err.Error(), "error in acquire hosts")
}

// TestReservationErrorInReservation simulates the error in ReserveHosts call
// from the HostService and verify that.
func (suite *ReserverTestSuite) TestReservationErrorInReservation() {
	task := createResMgrTask()
	hosts := []*models.Host{
		{
			Host: creareHostInfo(),
		},
	}
	suite.hostService.EXPECT().GetHosts(gomock.Any(), gomock.Any(), gomock.Any()).Return(hosts, nil)
	suite.hostService.EXPECT().ReserveHost(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("error in reserve hosts"))

	suite.reserver.GetReservationQueue().Enqueue(task)
	delay, err := suite.reserver.Reserve(context.Background())
	suite.Equal(delay.Seconds(), _noHostsTimeoutPenalty.Seconds())
	require.Error(suite.T(), err)
	suite.Contains(err.Error(), "error in reserve hosts")
}

// TestGetCompletedReservation tests the completed reservation call
func (suite *ReserverTestSuite) TestGetCompletedReservation() {
	reserver, _, completedQueue := suite.getReserver()
	// Testing if queue returns error
	completedQueue.EXPECT().Dequeue(gomock.Any()).Return(nil, errors.New("error"))
	_, err := reserver.GetCompletedReservation(context.Background())
	suite.Error(err)
	suite.Equal("error", err.Error())

	// testing if queue have a valid completed reservation
	completedQueue.EXPECT().Dequeue(gomock.Any()).Return(suite.getCompletedReservation(), nil)
	res, err := reserver.GetCompletedReservation(context.Background())
	suite.NoError(err)
	suite.Equal(len(res), 1)

	// testing if queue have a invalid item.
	var item interface{}
	completedQueue.EXPECT().Dequeue(gomock.Any()).Return(item, nil)
	res, err = reserver.GetCompletedReservation(context.Background())
	suite.Error(err)
	suite.Equal("invalid item in queue", err.Error())
	suite.Equal(len(res), 0)
}

// TestFindCompletedReservation tests the completed reservation
func (suite *ReserverTestSuite) TestFindCompletedReservation() {
	// testing hostservice completed reservation have error
	reserver, _, completedQueue := suite.getReserver()
	suite.hostService.EXPECT().GetCompletedReservation(gomock.Any()).Return(nil, errors.New("error"))
	err := reserver.enqueueCompletedReservation(context.Background())
	suite.Error(err)
	suite.Equal(err.Error(), "error")

	// testing hostservice completed reservation return no reservations
	var res []*hostsvc.CompletedReservation
	suite.hostService.EXPECT().GetCompletedReservation(gomock.Any()).Return(res, nil)
	err = reserver.enqueueCompletedReservation(context.Background())
	suite.NoError(err)

	// testing the valid completed reservation
	res = append(res, suite.getCompletedReservation())
	suite.hostService.EXPECT().GetCompletedReservation(gomock.Any()).Return(res, nil)
	completedQueue.EXPECT().Enqueue(gomock.Any()).Return(nil)
	err = reserver.enqueueCompletedReservation(context.Background())
	suite.NoError(err)

	// testing error in completed queue enqueue operation
	suite.hostService.EXPECT().GetCompletedReservation(gomock.Any()).Return(res, nil)
	completedQueue.EXPECT().Enqueue(gomock.Any()).Return(errors.New("error"))
	err = reserver.enqueueCompletedReservation(context.Background())
	suite.NoError(err)
}

// testing requeue when reservation failed.
func (suite *ReserverTestSuite) TestReserveAgain() {
	reserver, reserveQueue, _ := suite.getReserver()
	// testing the valid completed reservation
	var res []*hostsvc.CompletedReservation
	res = append(res, suite.getCompletedReservation())
	res[0].HostOffers = []*hostsvc.HostOffer{}
	suite.hostService.EXPECT().GetCompletedReservation(gomock.Any()).Return(res, nil)
	reserveQueue.EXPECT().Enqueue(gomock.Any()).Return(nil)
	err := reserver.enqueueCompletedReservation(context.Background())
	suite.NoError(err)

	// Testing requeue have error
	suite.hostService.EXPECT().GetCompletedReservation(gomock.Any()).Return(res, nil)
	reserveQueue.EXPECT().Enqueue(gomock.Any()).Return(errors.New("error"))
	err = reserver.enqueueCompletedReservation(context.Background())
	suite.NoError(err)
}

// TestEnqueueReservation tests the enqueue reservation
func (suite *ReserverTestSuite) TestEnqueueReservation() {
	reserver, reserveQueue, _ := suite.getReserver()
	err := reserver.EnqueueReservation(nil)
	suite.Error(err)
	suite.Equal(err.Error(), "invalid reservation")

	reserveQueue.EXPECT().Enqueue(gomock.Any()).Return(errors.New("error"))
	err = reserver.EnqueueReservation(&hostsvc.Reservation{})
	suite.Error(err)
	suite.Equal(err.Error(), "error")

	reserveQueue.EXPECT().Enqueue(gomock.Any()).Return(nil)
	err = reserver.EnqueueReservation(&hostsvc.Reservation{})
	suite.NoError(err)
}

func (suite *ReserverTestSuite) getReserver() (*reserver, *queue_mocks.MockQueue, *queue_mocks.MockQueue) {
	reserverQueue := queue_mocks.NewMockQueue(suite.mockCtrl)
	completedQueue := queue_mocks.NewMockQueue(suite.mockCtrl)
	metrics := metrics.NewMetrics(tally.NoopScope)

	suite.hostService = hosts_mock.NewMockService(suite.mockCtrl)
	config := &config.PlacementConfig{}
	return &reserver{
		metrics:                   metrics,
		config:                    config,
		hostService:               suite.hostService,
		reservationQueue:          reserverQueue,
		completedReservationQueue: completedQueue,
		reservations:              make(map[string][]*models.Host),
		tasks:                     make(map[string]*resmgr.Task),
	}, reserverQueue, completedQueue
}

func (suite *ReserverTestSuite) getCompletedReservation() *hostsvc.CompletedReservation {
	host := "host"
	return &hostsvc.CompletedReservation{
		HostOffers: []*hostsvc.HostOffer{
			{
				Hostname: host,
				AgentId: &mesos_v1.AgentID{
					Value: &host,
				},
			},
		},
		Task: &resmgr.Task{
			Id: &peloton.TaskID{
				Value: "task1",
			},
		},
	}
}

// TestTaskLen tests the TaskLen function which gets the
// length of the tasks in HostModel
func (suite *ReserverTestSuite) TestTaskLen() {
	host := &models.Host{
		Host: creareHostInfo(),
		Tasks: []*resmgr.Task{
			createResMgrTask(),
		},
	}
	suite.Equal(taskLen(host), 1)
}

func createResMgrTask() *resmgr.Task {
	return &resmgr.Task{
		Name:     "task",
		Hostname: "hostname",
		Type:     resmgr.TaskType_UNKNOWN,
		Id: &peloton.TaskID{
			Value: "task-1",
		},
		Constraint: &task.Constraint{},
	}
}

func creareHostInfo() *hostsvc.HostInfo {
	return &hostsvc.HostInfo{
		Hostname: "hostname",
	}
}
