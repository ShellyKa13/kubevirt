/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package rest

import (
	"fmt"

	"github.com/emicklei/go-restful/v3"

	"k8s.io/apimachinery/pkg/api/errors"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"
)

func (app *SubresourceAPIApp) BackupVMRequestHandler(request *restful.Request, response *restful.Response) {

	log.Log.Info("BackupVMRequestHandler")
	// name := request.PathParameter("name")
	// namespace := request.PathParameter("namespace")

	// vmi, err := app.virtCli.VirtualMachineInstance(namespace).Get(context.Background(), name, k8smetav1.GetOptions{})
	// if err != nil && errors.IsNotFound(err) {
	// 	writeError(errors.NewConflict(v1.Resource("virtualmachine"), name, fmt.Errorf("VM is not running: %v", v1.RunStrategyHalted)), response)
	// 	return
	// } else if err != nil {
	// 	writeError(errors.NewInternalError(err), response)
	// 	return
	// }

	validate := func(vmi *v1.VirtualMachineInstance) *errors.StatusError {
		if vmi.Status.Phase != v1.Running {
			return errors.NewConflict(v1.Resource("virtualmachineinstance"), vmi.Name, fmt.Errorf(vmNotRunning))
		}
		return nil
	}

	getURL := func(vmi *v1.VirtualMachineInstance, conn kubecli.VirtHandlerConn) (string, error) {
		return conn.BackupURI(vmi)
	}

	app.putRequestHandler(request, response, validate, getURL, false)
}
