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

package ptoa_test

import (
	"testing"

	"github.com/uber/peloton/.gen/peloton/api/v1alpha/peloton"
	"github.com/uber/peloton/.gen/thrift/aurora/api"

	"github.com/uber/peloton/pkg/aurorabridge/atop"
	"github.com/uber/peloton/pkg/aurorabridge/common"
	"github.com/uber/peloton/pkg/aurorabridge/fixture"
	"github.com/uber/peloton/pkg/aurorabridge/label"

	"github.com/stretchr/testify/assert"
	"go.uber.org/thriftrw/ptr"

	. "github.com/uber/peloton/pkg/aurorabridge/ptoa"
)

// TestNewConstraints_Nil tests that NewConstraints returns
// nil for nil input, and does not throw error
func TestNewConstraints_Nil(t *testing.T) {
	cc, err := NewConstraints(nil)
	assert.NoError(t, err)
	assert.Nil(t, cc)
}

// TestNewConstraints_LimitConstraint tests that NewConstraints
// returns aurora LimitConstraint correctly based on input generated
// by atop.NewConstraint()
func TestNewConstraints_LimitConstraint(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobKeyLabel := label.NewAuroraJobKey(jobKey)
	constraints := []*api.Constraint{
		{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Limit: &api.LimitConstraint{
					Limit: ptr.Int32(5),
				},
			},
		},
	}

	c, err := atop.NewConstraint(jobKeyLabel, constraints)
	assert.NoError(t, err)

	cc, err := NewConstraints(c)
	assert.NoError(t, err)

	assert.Equal(t, constraints, cc)
}

// TestNewConstraints_LimitConstraint_InvalidLabel tests that
// NewConstraints will return an error if the input constraint
// does not have a valid Label when processing for LimitConstraint.
func TestNewConstraints_LimitConstraint_InvalidLabel(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobKeyLabel := label.NewAuroraJobKey(jobKey)
	constraints := []*api.Constraint{
		{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Limit: &api.LimitConstraint{
					Limit: ptr.Int32(5),
				},
			},
		},
	}

	c, err := atop.NewConstraint(jobKeyLabel, constraints)
	assert.NoError(t, err)

	c.GetLabelConstraint().Label = &peloton.Label{
		Value: "invalid",
	}

	cc, err := NewConstraints(c)
	assert.Error(t, err)
	assert.Nil(t, cc)
}

// TestNewConstraints_ValueConstraintSingle tests that NewConstraints
// returns aurora ValueConstraint with single value correctly based
// on input generated by atop.NewConstraint()
func TestNewConstraints_ValueConstraintSingle(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobKeyLabel := label.NewAuroraJobKey(jobKey)
	constraints := []*api.Constraint{
		{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Value: &api.ValueConstraint{
					Values: map[string]struct{}{
						"host-1": {},
					},
				},
			},
		},
	}

	c, err := atop.NewConstraint(jobKeyLabel, constraints)
	assert.NoError(t, err)

	cc, err := NewConstraints(c)
	assert.NoError(t, err)

	assert.Equal(t, constraints, cc)
}

// TestNewConstraints_ValueConstraintMultiple tests that NewConstraints
// returns aurora ValueConstraint with multiple values correctly based
// on input generated by atop.NewConstraint()
func TestNewConstraints_ValueConstraintMultiple(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobKeyLabel := label.NewAuroraJobKey(jobKey)
	constraints := []*api.Constraint{
		{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Value: &api.ValueConstraint{
					Values: map[string]struct{}{
						"host-1": {},
						"host-2": {},
					},
				},
			},
		},
	}

	c, err := atop.NewConstraint(jobKeyLabel, constraints)
	assert.NoError(t, err)

	cc, err := NewConstraints(c)
	assert.NoError(t, err)

	assert.Equal(t, constraints, cc)
}

// TestNewConstraints_MultipleConstraints tests that NewConstraints
// returns multiple aurora Constraints correctly based on input generated
// by atop.NewConstraint()
func TestNewConstraints_MultipleConstraints(t *testing.T) {
	jobKey := fixture.AuroraJobKey()
	jobKeyLabel := label.NewAuroraJobKey(jobKey)
	constraints := []*api.Constraint{
		{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Value: &api.ValueConstraint{
					Values: map[string]struct{}{
						"host-1": {},
						"host-2": {},
					},
				},
			},
		},
		{
			Name: ptr.String(common.MesosHostAttr),
			Constraint: &api.TaskConstraint{
				Limit: &api.LimitConstraint{
					Limit: ptr.Int32(5),
				},
			},
		},
	}

	c, err := atop.NewConstraint(jobKeyLabel, constraints)
	assert.NoError(t, err)

	cc, err := NewConstraints(c)
	assert.NoError(t, err)

	assert.Equal(t, constraints, cc)
}
