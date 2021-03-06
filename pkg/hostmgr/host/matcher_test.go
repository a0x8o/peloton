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

package host

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	mesos "github.com/uber/peloton/.gen/mesos/v1"
	mesos_master "github.com/uber/peloton/.gen/mesos/v1/master"
	hpb "github.com/uber/peloton/.gen/peloton/api/v0/host"
	"github.com/uber/peloton/.gen/peloton/api/v0/peloton"
	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/hostmgr/hostsvc"

	"github.com/uber/peloton/pkg/common"
	"github.com/uber/peloton/pkg/common/constraints"
	constraint_mocks "github.com/uber/peloton/pkg/common/constraints/mocks"
	"github.com/uber/peloton/pkg/common/util"
	hm "github.com/uber/peloton/pkg/hostmgr/host/mocks"
	mock_mpb "github.com/uber/peloton/pkg/hostmgr/mesos/yarpc/encoding/mpb/mocks"
	"github.com/uber/peloton/pkg/hostmgr/scalar"
)

var (
	_zeroResourceValue = 0
)

type MatcherTestSuite struct {
	suite.Suite

	ctrl               *gomock.Controller
	testScope          tally.TestScope
	operatorClient     *mock_mpb.MockMasterOperatorClient
	response           *mesos_master.Response_GetAgents
	mockMaintenanceMap *hm.MockMaintenanceHostInfoMap
}

func (suite *MatcherTestSuite) SetupTest() {
	suite.ctrl = gomock.NewController(suite.T())
	suite.testScope = tally.NewTestScope("", map[string]string{})
	suite.operatorClient = mock_mpb.NewMockMasterOperatorClient(suite.ctrl)
	suite.mockMaintenanceMap = hm.NewMockMaintenanceHostInfoMap(suite.ctrl)
	suite.InitializeHosts()
}

// InitializeHosts creates the host map for mesos agents
func (suite *MatcherTestSuite) InitializeHosts() {
	loader := &Loader{
		OperatorClient:         suite.operatorClient,
		Scope:                  suite.testScope,
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
	numAgents := 2
	suite.response = makeAgentsResponse(numAgents)
	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(suite.response, nil),

		suite.mockMaintenanceMap.EXPECT().
			GetDrainingHostInfos(gomock.Any()).
			Return([]*hpb.HostInfo{}).
			Times(len(suite.response.GetAgents())),
	)

	loader.Load(nil)
}

func getNewMatcher(
	filter *hostsvc.HostFilter,
	evaluator constraints.Evaluator) *Matcher {
	resourceTypeFilter := func(resourceType string) bool {
		if resourceType == common.MesosCPU {
			return true
		}
		return false
	}
	return NewMatcher(filter, evaluator, resourceTypeFilter)
}

// getAgentResponse generates the agent response
func getAgentResponse(hostname string, resval float64) *mesos_master.Response_GetAgents_Agent {
	resVal := resval
	tmpID := hostname
	resources := []*mesos.Resource{
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(resVal).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosMem).
			WithValue(resVal).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosDisk).
			WithValue(resVal).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosGPU).
			WithValue(resVal).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosCPU).
			WithValue(resVal).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			Build(),
		util.NewMesosResourceBuilder().
			WithName(common.MesosMem).
			WithValue(resVal).
			WithRevocable(&mesos.Resource_RevocableInfo{}).
			Build(),
	}
	return &mesos_master.Response_GetAgents_Agent{
		AgentInfo: &mesos.AgentInfo{
			Hostname:  &tmpID,
			Resources: resources,
		},
		TotalResources: resources,
	}
}

// createAgentsResponse takes the number of agents and create agentresponse
func createAgentsResponse(numAgents int, sameResource bool) *mesos_master.Response_GetAgents {
	response := &mesos_master.Response_GetAgents{
		Agents: []*mesos_master.Response_GetAgents_Agent{},
	}
	res := _zeroResourceValue
	if !sameResource {
		res = _defaultResourceValue
	}
	response.Agents = append(response.Agents, getAgentResponse("id-1", float64(res)))
	response.Agents = append(response.Agents, getAgentResponse("id-2", float64(_zeroResourceValue)))
	return response
}

func TestMatcherTestSuite(t *testing.T) {
	suite.Run(t, new(MatcherTestSuite))
}

// TestResourcesConstraint tests the different return codes from matchHostFilter
func (suite *MatcherTestSuite) TestResourcesConstraint() {
	defer suite.ctrl.Finish()

	filter := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: 1,
		},
		ResourceConstraint: &hostsvc.ResourceConstraint{
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
			},
			Revocable: true,
		},
	}
	matcher := getNewMatcher(filter, nil)
	hostname := suite.response.Agents[0].AgentInfo.GetHostname()
	agents, err := matcher.GetMatchingHosts()
	suite.Equal(len(agents), 2)
	suite.Nil(err)

	agentNonRevocableResources, _ := scalar.FilterMesosResources(
		suite.response.Agents[0].AgentInfo.Resources,
		func(r *mesos.Resource) bool {
			if r.GetRevocable() == nil {
				return true
			}

			return false
		})

	resources := scalar.FromMesosResources(agentNonRevocableResources)

	testTable := []struct {
		hostname  string
		resources scalar.Resources
		expected  hostsvc.HostFilterResult
		filter    *hostsvc.HostFilter
		msg       string
	}{
		{
			msg:       "Enough resource with GPU",
			expected:  hostsvc.HostFilterResult_MATCH,
			hostname:  hostname,
			resources: resources,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
						GpuLimit:    1.0,
					},
				},
			},
		},
		{
			msg:       "Not Enough CPU Resources.",
			expected:  hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES,
			hostname:  hostname,
			resources: resources,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					NumPorts: uint32(2),
					Minimum: &task.ResourceConfig{
						CpuLimit:    2.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
						GpuLimit:    1.0,
					},
				},
			},
		},
		{
			msg:       "Not enough memory",
			expected:  hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES,
			hostname:  hostname,
			resources: resources,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  2.0,
						DiskLimitMb: 1.0,
						GpuLimit:    1.0,
					},
				},
			},
		},
		{
			msg:       "Enough resource without GPU",
			expected:  hostsvc.HostFilterResult_MATCH,
			hostname:  hostname,
			resources: resources,
			filter: &hostsvc.HostFilter{
				Quantity: &hostsvc.QuantityControl{
					MaxHosts: 1,
				},
				ResourceConstraint: &hostsvc.ResourceConstraint{
					Minimum: &task.ResourceConfig{
						CpuLimit:    1.0,
						MemLimitMb:  1.0,
						DiskLimitMb: 1.0,
					},
				},
			},
		},
	}

	for _, tt := range testTable {
		suite.Equal(
			tt.expected,
			matcher.matchHostFilter(
				tt.hostname,
				tt.resources,
				tt.filter,
				nil,
				GetAgentMap()),
			tt.msg,
		)
	}
}

// TestHostConstraints tests the constraints from matchHostFilter
func (suite *MatcherTestSuite) TestHostConstraints() {
	testTable := map[string]struct {
		match       hostsvc.HostFilterResult
		evaluateRes constraints.EvaluateResult
		evaluateErr error
	}{
		"matched-correctly": {
			match:       hostsvc.HostFilterResult_MATCH,
			evaluateRes: constraints.EvaluateResultMatch,
		},
		"matched-not-applicable": {
			match:       hostsvc.HostFilterResult_MATCH,
			evaluateRes: constraints.EvaluateResultNotApplicable,
		},
		"mismatched-constraint": {
			match:       hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
			evaluateRes: constraints.EvaluateResultMismatch,
		},
		"mismatched-error": {
			match:       hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
			evaluateErr: errors.New("some error"),
		},
	}

	for ttName, tt := range testTable {
		ctrl := gomock.NewController(suite.T())
		mockEvaluator := constraint_mocks.NewMockEvaluator(ctrl)
		filter := &hostsvc.HostFilter{
			SchedulingConstraint: &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind: task.LabelConstraint_TASK,
				},
			},
		}
		agentInfo := suite.response.GetAgents()[0].AgentInfo
		lv := constraints.GetHostLabelValues(agentInfo.GetHostname(), agentInfo.GetAttributes())
		mockEvaluator.
			EXPECT().
			Evaluate(
				gomock.Eq(filter.SchedulingConstraint),
				gomock.Eq(lv)).
			Return(tt.evaluateRes, tt.evaluateErr)
		matcher := getNewMatcher(filter, mockEvaluator)
		result := matcher.matchHostFilter(suite.response.Agents[0].AgentInfo.GetHostname(),
			scalar.FromMesosResources(suite.response.Agents[0].AgentInfo.Resources), filter, mockEvaluator, GetAgentMap())
		suite.Equal(result, tt.match, "test case is %s", ttName)
	}
}

// TestMatchHostsFilter matches the host filter to available nodes
func (suite *MatcherTestSuite) TestMatchHostsFilter() {
	res := createAgentResourceMap(nil, nil, nil)
	suite.Nil(res)

	filter := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: 1,
		},
		ResourceConstraint: &hostsvc.ResourceConstraint{
			NumPorts: uint32(2),
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
				GpuLimit:    1.0,
			},
		},
	}
	matcher := getNewMatcher(filter, nil)
	// Checking with valid host filter
	result := matcher.matchHostsFilter(matcher.agentMap, filter, nil, GetAgentMap())
	suite.Equal(result, hostsvc.HostFilterResult_MATCH)
	hosts, err := matcher.GetMatchingHosts()
	suite.Nil(err)
	// hostfilter should return both the hosts
	suite.Equal(len(hosts), 2)
	// invalid agent Map
	result = matcher.matchHostsFilter(nil, filter, nil, GetAgentMap())
	suite.Equal(result, hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES)
	// invalid agentInfoMap
	result = matcher.matchHostsFilter(matcher.agentMap, filter, nil, nil)
	suite.Equal(result, hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES)
}

// TestMatchHostsFilterWithDifferentosts tests with different kind of hosts
func (suite *MatcherTestSuite) TestMatchHostsFilterWithDifferentHosts() {
	// Creating different resources hosts in the host map
	loader := &Loader{
		OperatorClient:         suite.operatorClient,
		Scope:                  suite.testScope,
		SlackResourceTypes:     []string{common.MesosCPU},
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
	numAgents := 2
	response := createAgentsResponse(numAgents, false)

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(response, nil),

		suite.mockMaintenanceMap.EXPECT().
			GetDrainingHostInfos(gomock.Any()).
			Return([]*hpb.HostInfo{}).
			Times(len(suite.response.GetAgents())),
	)

	loader.Load(nil)
	filter := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: 1,
		},
		ResourceConstraint: &hostsvc.ResourceConstraint{
			NumPorts: uint32(2),
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
				GpuLimit:    1.0,
			},
		},
	}
	matcher := getNewMatcher(filter, nil)
	// one of the host should match with this filter
	result := matcher.matchHostsFilter(matcher.agentMap, filter, nil, GetAgentMap())
	suite.Equal(result, hostsvc.HostFilterResult_MATCH)
	hosts, err := matcher.GetMatchingHosts()
	suite.Nil(err)
	// one host matched
	suite.Equal(len(hosts), 1)
}

// TestMatchHostsFilterWithZeroResourceHosts tests hosts with not sufficient resources
func (suite *MatcherTestSuite) TestMatchHostsFilterWithZeroResourceHosts() {
	// Creating host map with not sufficient resources
	loader := &Loader{
		OperatorClient:         suite.operatorClient,
		Scope:                  suite.testScope,
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
	numAgents := 2
	response := createAgentsResponse(numAgents, true)

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(response, nil),

		suite.mockMaintenanceMap.EXPECT().
			GetDrainingHostInfos(gomock.Any()).
			Return([]*hpb.HostInfo{}).
			Times(len(suite.response.GetAgents())),
	)

	loader.Load(nil)
	filter := &hostsvc.HostFilter{
		Quantity: &hostsvc.QuantityControl{
			MaxHosts: 1,
		},
		ResourceConstraint: &hostsvc.ResourceConstraint{
			NumPorts: uint32(2),
			Minimum: &task.ResourceConfig{
				CpuLimit:    1.0,
				MemLimitMb:  1.0,
				DiskLimitMb: 1.0,
				GpuLimit:    1.0,
			},
		},
	}
	matcher := getNewMatcher(filter, nil)
	result := matcher.matchHostsFilter(matcher.agentMap, filter, nil, GetAgentMap())
	suite.Equal(result, hostsvc.HostFilterResult_INSUFFICIENT_RESOURCES)
	hosts, err := matcher.GetMatchingHosts()
	suite.NotNil(err)
	// this should return error and matching error contents.
	suite.Contains(err.Message, "could not return matching hosts")
	suite.Nil(hosts)
}

// TestMatchHostsFilterExclusiveHosts tests filtering of exclusive hosts
func (suite *MatcherTestSuite) TestMatchHostsFilterExclusiveHosts() {
	loader := &Loader{
		OperatorClient:         suite.operatorClient,
		Scope:                  suite.testScope,
		MaintenanceHostInfoMap: suite.mockMaintenanceMap,
	}
	response := createAgentsResponse(2, true)
	// Make agent at index[0] to have exclusive attribute
	exclAttrName := "peloton/exclusive"
	exclAttrValue := "web-tier"
	textType := mesos.Value_TEXT
	response.GetAgents()[0].GetAgentInfo().Attributes = []*mesos.Attribute{
		{
			Name: &exclAttrName,
			Text: &mesos.Value_Text{
				Value: &exclAttrValue,
			},
			Type: &textType,
		},
	}

	gomock.InOrder(
		suite.operatorClient.EXPECT().Agents().Return(response, nil),

		suite.mockMaintenanceMap.EXPECT().
			GetDrainingHostInfos(gomock.Any()).
			Return([]*hpb.HostInfo{}).
			Times(len(suite.response.GetAgents())),
	)

	loader.Load(nil)

	testTable := []struct {
		msg            string
		agentIndex     int
		exclConstraint bool
		expected       hostsvc.HostFilterResult
	}{
		{
			msg:            "excl host, excl constraint -> match",
			agentIndex:     0,
			exclConstraint: true,
			expected:       hostsvc.HostFilterResult_MATCH,
		},
		{
			msg:            "excl host, non-excl constraint -> mismatch",
			agentIndex:     0,
			exclConstraint: false,
			expected:       hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
		},
		{
			msg:            "non-excl host, excl constraint -> mismatch",
			agentIndex:     1,
			exclConstraint: true,
			expected:       hostsvc.HostFilterResult_MISMATCH_CONSTRAINTS,
		},
		{
			msg:            "non-excl host, non-excl constraint -> match",
			agentIndex:     1,
			exclConstraint: false,
			expected:       hostsvc.HostFilterResult_MATCH,
		},
	}

	for _, tt := range testTable {
		agentInfo := response.GetAgents()[tt.agentIndex].GetAgentInfo()
		resources := scalar.FromMesosResources(agentInfo.GetResources())
		hostname := agentInfo.GetHostname()

		filter := &hostsvc.HostFilter{
			ResourceConstraint: &hostsvc.ResourceConstraint{
				Minimum: &task.ResourceConfig{},
			},
		}
		if tt.exclConstraint {
			filter.SchedulingConstraint = &task.Constraint{
				Type: task.Constraint_LABEL_CONSTRAINT,
				LabelConstraint: &task.LabelConstraint{
					Kind: task.LabelConstraint_HOST,
					Label: &peloton.Label{
						Key:   "peloton/exclusive",
						Value: "web-tier",
					},
					Condition:   task.LabelConstraint_CONDITION_EQUAL,
					Requirement: 1,
				},
			}
		}
		evaluator := constraints.NewEvaluator(task.LabelConstraint_HOST)
		matcher := getNewMatcher(filter, evaluator)
		suite.Equal(
			tt.expected,
			matcher.matchHostFilter(
				hostname,
				resources,
				filter,
				evaluator,
				GetAgentMap()),
			tt.msg,
		)
	}
}
