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

package task

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/uber/peloton/.gen/peloton/api/v0/task"
	"github.com/uber/peloton/.gen/peloton/private/resmgrsvc"

	taskutil "github.com/uber/peloton/pkg/common/util/task"
	jobmgrcommon "github.com/uber/peloton/pkg/jobmgr/common"
)

// EnqueueGangs enqueues all tasks organized in gangs to respool in resmgr.
func EnqueueGangs(
	ctx context.Context,
	tasks []*task.TaskInfo,
	jobConfig jobmgrcommon.JobConfig,
	client resmgrsvc.ResourceManagerServiceYARPCClient) error {
	ctxWithTimeout, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
	defer cancelFunc()

	gangs := taskutil.ConvertToResMgrGangs(tasks, jobConfig)
	var request = &resmgrsvc.EnqueueGangsRequest{
		Gangs:   gangs,
		ResPool: jobConfig.GetRespoolID(),
	}

	response, err := client.EnqueueGangs(ctxWithTimeout, request)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"request": request,
		}).Error("resource manager enqueue gangs failed")
		return err
	}
	if response.GetError() != nil {
		log.WithFields(log.Fields{
			"resp_error": response.GetError().String(),
			"request":    request,
		}).Error("resource manager enqueue gangs response error")
		return errors.New(response.GetError().String())
	}
	log.WithField("count", len(tasks)).
		Debug("Enqueued tasks as gangs to Resource Manager")
	return nil
}
