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
	"context"
	"fmt"
	"strings"

	k8sv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	k8srest "k8s.io/apiserver/pkg/registry/rest"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/apimachinery/patch"
	"kubevirt.io/kubevirt/pkg/controller"
	"kubevirt.io/kubevirt/pkg/pointer"
)

type VirtualMachineREST struct {
	virtCli kubecli.KubevirtClient
}

func NewVirtualMachineREST(virtCli kubecli.KubevirtClient) *VirtualMachineREST {
	return &VirtualMachineREST{virtCli: virtCli}
}

var _ k8srest.Getter = &VirtualMachineREST{}
var _ k8srest.Scoper = &VirtualMachineREST{}
var _ k8srest.Storage = &VirtualMachineREST{}

func (r *VirtualMachineREST) New() runtime.Object   { return &v1.VirtualMachine{} }
func (r *VirtualMachineREST) Destroy()              {}
func (r *VirtualMachineREST) NamespaceScoped() bool { return true }

func (r *VirtualMachineREST) Get(
	ctx context.Context,
	name string,
	opts *metav1.GetOptions,
) (runtime.Object, error) {
	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}
	return r.virtCli.VirtualMachine(namespace).Get(ctx, name, *opts)
}

type VirtualMachineInstanceREST struct {
	virtCli kubecli.KubevirtClient
}

func NewVirtualMachineInstanceREST(virtCli kubecli.KubevirtClient) *VirtualMachineInstanceREST {
	return &VirtualMachineInstanceREST{virtCli: virtCli}
}

var _ k8srest.Getter = &VirtualMachineInstanceREST{}
var _ k8srest.Scoper = &VirtualMachineInstanceREST{}
var _ k8srest.Storage = &VirtualMachineInstanceREST{}
var _ k8srest.SingularNameProvider = &VirtualMachineREST{}
var _ k8srest.SingularNameProvider = &VirtualMachineInstanceREST{}

func (r *VirtualMachineInstanceREST) New() runtime.Object   { return &v1.VirtualMachineInstance{} }
func (r *VirtualMachineInstanceREST) Destroy()              {}
func (r *VirtualMachineInstanceREST) NamespaceScoped() bool { return true }

func (r *VirtualMachineREST) GetSingularName() string {
    return "virtualmachine"
}

func (r *VirtualMachineInstanceREST) GetSingularName() string {
    return "virtualmachineinstance"
}
func (r *VirtualMachineInstanceREST) Get(
	ctx context.Context,
	name string,
	opts *metav1.GetOptions,
) (runtime.Object, error) {
	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}
	return r.virtCli.VirtualMachineInstance(namespace).Get(ctx, name, *opts)
}

type StartREST struct {
	virtCli kubecli.KubevirtClient
}

func NewStartREST(virtCli kubecli.KubevirtClient) *StartREST {
	return &StartREST{virtCli: virtCli}
}

var _ k8srest.NamedCreater = &StartREST{}
var _ k8srest.Scoper = &StartREST{}
var _ k8srest.Storage = &StartREST{}

func (r *StartREST) New() runtime.Object {
	return &v1.StartOptions{}
}

func (r *StartREST) Destroy() {}

func (r *StartREST) NamespaceScoped() bool {
	return true
}

func (r *StartREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vm, err := r.virtCli.VirtualMachine(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(v1.Resource("virtualmachines"), name)
		}
		return nil, errors.NewInternalError(err)
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx,
		name,
		metav1.GetOptions{},
	)
	if err != nil && !errors.IsNotFound(err) {
		return nil, errors.NewInternalError(err)
	}

	if vmi != nil && !vmi.IsFinal() &&
		vmi.Status.Phase != v1.Unknown &&
		vmi.Status.Phase != v1.VmPhaseUnset {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"),
			name,
			fmt.Errorf("VM is already running"),
		)
	}

	if controller.NewVirtualMachineConditionManager().HasConditionWithStatus(
		vm, v1.VirtualMachineManualRecoveryRequired, k8sv1.ConditionTrue,
	) {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"),
			name,
			fmt.Errorf(volumeMigrationManualRecoveryRequiredErr),
		)
	}

	bodyStruct := &v1.StartOptions{}
	if obj != nil {
		var ok bool
		bodyStruct, ok = obj.(*v1.StartOptions)
		if !ok {
			return nil, errors.NewBadRequest("invalid request body type")
		}
	}

	startPaused := bodyStruct.Paused
	startChangeRequestData := make(map[string]string)
	if startPaused {
		startChangeRequestData[v1.StartRequestDataPausedKey] = v1.StartRequestDataPausedTrue
	}

	var patchErr error

	runStrategy, err := vm.RunStrategy()
	if err != nil {
		return nil, errors.NewInternalError(err)
	}

	switch runStrategy {
	case v1.RunStrategyHalted:
		pausedStartStrategy := v1.StartStrategyPaused
		if startPaused && (vm.Spec.Template == nil ||
			vm.Spec.Template.Spec.StartStrategy != &pausedStartStrategy) {
			patchBytes, err := getChangeRequestJson(vm,
				v1.VirtualMachineStateChangeRequest{
					Action: v1.StartRequest,
					Data:   startChangeRequestData,
				},
			)
			if err != nil {
				return nil, errors.NewInternalError(err)
			}
			log.Log.Object(vm).V(4).Infof(patchingVMStatusFmt, string(patchBytes))
			_, patchErr = r.virtCli.VirtualMachine(vm.Namespace).PatchStatus(
				ctx,
				vm.Name,
				types.JSONPatchType,
				patchBytes,
				metav1.PatchOptions{DryRun: bodyStruct.DryRun},
			)
		} else {
			patchBytes, err := getRunningPatch(vm, true)
			if err != nil {
				return nil, errors.NewInternalError(err)
			}
			log.Log.Object(vm).V(4).Infof(patchingVMFmt, string(patchBytes))
			_, patchErr = r.virtCli.VirtualMachine(namespace).Patch(
				ctx,
				vm.GetName(),
				types.JSONPatchType,
				patchBytes,
				metav1.PatchOptions{DryRun: bodyStruct.DryRun},
			)
		}

	case v1.RunStrategyRerunOnFailure, v1.RunStrategyManual:
		needsRestart := false
		if (runStrategy == v1.RunStrategyRerunOnFailure &&
			vmi != nil && vmi.Status.Phase == v1.Succeeded) ||
			(runStrategy == v1.RunStrategyManual &&
				vmi != nil && vmi.IsFinal()) {
			needsRestart = true
		} else if runStrategy == v1.RunStrategyRerunOnFailure &&
			vmi != nil && vmi.Status.Phase == v1.Failed {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name,
				fmt.Errorf("%v does not support starting VM from failed state",
					v1.RunStrategyRerunOnFailure),
			)
		}

		var patchBytes []byte
		if needsRestart {
			patchBytes, err = getChangeRequestJson(vm,
				v1.VirtualMachineStateChangeRequest{Action: v1.StopRequest, UID: &vmi.UID},
				v1.VirtualMachineStateChangeRequest{Action: v1.StartRequest, Data: startChangeRequestData},
			)
		} else {
			patchBytes, err = getChangeRequestJson(vm,
				v1.VirtualMachineStateChangeRequest{Action: v1.StartRequest, Data: startChangeRequestData},
			)
		}
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		log.Log.Object(vm).V(4).Infof(patchingVMStatusFmt, string(patchBytes))
		_, patchErr = r.virtCli.VirtualMachine(vm.Namespace).PatchStatus(
			ctx,
			vm.Name,
			types.JSONPatchType,
			patchBytes,
			metav1.PatchOptions{DryRun: bodyStruct.DryRun},
		)

	case v1.RunStrategyAlways, v1.RunStrategyOnce:
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"), name,
			fmt.Errorf("%v does not support manual start requests", runStrategy),
		)
	}

	if patchErr != nil {
		if strings.Contains(patchErr.Error(), jsonpatchTestErr) {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name, patchErr,
			)
		}
		return nil, errors.NewInternalError(patchErr)
	}

	return vm, nil
}

type StopREST struct {
	virtCli kubecli.KubevirtClient
}

func NewStopREST(virtCli kubecli.KubevirtClient) *StopREST {
	return &StopREST{virtCli: virtCli}
}

var _ k8srest.NamedCreater = &StopREST{}
var _ k8srest.Scoper = &StopREST{}
var _ k8srest.Storage = &StopREST{}

func (r *StopREST) New() runtime.Object { return &v1.StopOptions{} }
func (r *StopREST) Destroy()            {}

func (r *StopREST) NamespaceScoped() bool { return true }

func (r *StopREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	bodyStruct := &v1.StopOptions{}
	if obj != nil {
		var ok bool
		bodyStruct, ok = obj.(*v1.StopOptions)
		if !ok {
			return nil, errors.NewBadRequest("invalid request body")
		}
	}

	vm, err := r.virtCli.VirtualMachine(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachines"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	runStrategy, err := vm.RunStrategy()
	if err != nil {
		return nil, errors.NewInternalError(err)
	}

	hasVMI := true
	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx,
		name, metav1.GetOptions{},
	)
	if err != nil && errors.IsNotFound(err) {
		hasVMI = false
	} else if err != nil {
		return nil, errors.NewInternalError(err)
	}

	var oldGracePeriodSeconds int64
	var patchErr error

	if hasVMI && !vmi.IsFinal() && bodyStruct.GracePeriod != nil {
		patchSet := patch.New()
		if vmi.Spec.TerminationGracePeriodSeconds != nil {
			oldGracePeriodSeconds = *vmi.Spec.TerminationGracePeriodSeconds
			patchSet.AddOption(patch.WithTest(
				"/spec/terminationGracePeriodSeconds",
				*vmi.Spec.TerminationGracePeriodSeconds,
			))
		} else {
			patchSet.AddOption(patch.WithTest(
				"/spec/terminationGracePeriodSeconds", nil,
			))
		}
		patchSet.AddOption(patch.WithReplace(
			"/spec/terminationGracePeriodSeconds",
			*bodyStruct.GracePeriod,
		))
		patchBytes, err := patchSet.GeneratePayload()
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		log.Log.Object(vmi).V(2).Infof("Patching VMI: %s", string(patchBytes))
		_, err = r.virtCli.VirtualMachineInstance(namespace).Patch(
			ctx,
			vmi.GetName(),
			types.JSONPatchType,
			patchBytes,
			metav1.PatchOptions{DryRun: bodyStruct.DryRun},
		)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
	}

	switch runStrategy {
	case v1.RunStrategyHalted:
		if !hasVMI || vmi.IsFinal() {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name,
				fmt.Errorf(vmNotRunning),
			)
		}
		if bodyStruct.GracePeriod == nil ||
			(vmi.Spec.TerminationGracePeriodSeconds != nil &&
				*bodyStruct.GracePeriod >= oldGracePeriodSeconds) {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name,
				fmt.Errorf("%v only supports manual stop requests with a shorter graceperiod",
					v1.RunStrategyHalted),
			)
		}
		patchBytes, err := getChangeRequestJson(vm,
			v1.VirtualMachineStateChangeRequest{
				Action: v1.StopRequest,
				UID:    &vmi.UID,
			},
		)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		log.Log.Object(vm).V(4).Infof(patchingVMStatusFmt, string(patchBytes))
		_, patchErr = r.virtCli.VirtualMachine(vm.Namespace).PatchStatus(
			ctx, vm.Name, types.JSONPatchType, patchBytes,
			metav1.PatchOptions{DryRun: bodyStruct.DryRun},
		)

	case v1.RunStrategyRerunOnFailure, v1.RunStrategyManual:
		if !hasVMI || vmi.IsFinal() {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name,
				fmt.Errorf(vmNotRunning),
			)
		}
		patchBytes, err := getChangeRequestJson(vm,
			v1.VirtualMachineStateChangeRequest{
				Action: v1.StopRequest,
				UID:    &vmi.UID,
			},
		)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		log.Log.Object(vm).V(4).Infof(patchingVMStatusFmt, string(patchBytes))
		_, patchErr = r.virtCli.VirtualMachine(vm.Namespace).PatchStatus(
			ctx, vm.Name, types.JSONPatchType, patchBytes,
			metav1.PatchOptions{DryRun: bodyStruct.DryRun},
		)

	case v1.RunStrategyAlways, v1.RunStrategyOnce:
		patchBytes, err := getRunningPatch(vm, false)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		log.Log.Object(vm).V(4).Infof(patchingVMFmt, string(patchBytes))
		_, patchErr = r.virtCli.VirtualMachine(namespace).Patch(
			ctx, vm.GetName(), types.JSONPatchType, patchBytes,
			metav1.PatchOptions{DryRun: bodyStruct.DryRun},
		)
	}

	if patchErr != nil {
		if strings.Contains(patchErr.Error(), jsonpatchTestErr) {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name, patchErr,
			)
		}
		return nil, errors.NewInternalError(patchErr)
	}

	return vm, nil
}

type PauseREST struct {
	virtCli kubecli.KubevirtClient
	// connFactory injects connection logic from dialers.go
	// keeps PauseREST independent of TLS/port details
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewPauseREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *PauseREST {
	return &PauseREST{
		virtCli:     virtCli,
		connFactory: connFactory,
	}
}

var _ k8srest.NamedCreater = &PauseREST{}
var _ k8srest.Scoper = &PauseREST{}
var _ k8srest.Storage = &PauseREST{}

func (r *PauseREST) New() runtime.Object   { return &v1.PauseOptions{} }
func (r *PauseREST) Destroy()              {}
func (r *PauseREST) NamespaceScoped() bool { return true }

func (r *PauseREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	// decode body
	bodyStruct := &v1.PauseOptions{}
	if obj != nil {
		var ok bool
		bodyStruct, ok = obj.(*v1.PauseOptions)
		if !ok {
			return nil, errors.NewBadRequest("invalid request body")
		}
	}

	// get VMI — replaces fetchAndValidateVirtualMachineInstance
	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmNotRunning),
		)
	}
	if vmi.Spec.LivenessProbe != nil {
		return nil, errors.NewForbidden(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf("Pausing VMIs with LivenessProbe is currently not supported"),
		)
	}
	condManager := controller.NewVirtualMachineInstanceConditionManager()
	if condManager.HasCondition(vmi, v1.VirtualMachineInstancePaused) {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf("VMI is already paused"),
		)
	}

	dryRun := len(bodyStruct.DryRun) > 0 &&
		bodyStruct.DryRun[0] == metav1.DryRunAll
	if dryRun {
		return vmi, nil
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.PauseURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get pause URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

type UnpauseREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewUnpauseREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *UnpauseREST {
	return &UnpauseREST{
		virtCli:     virtCli,
		connFactory: connFactory,
	}
}

var _ k8srest.NamedCreater = &UnpauseREST{}
var _ k8srest.Scoper = &UnpauseREST{}
var _ k8srest.Storage = &UnpauseREST{}

func (r *UnpauseREST) New() runtime.Object   { return &v1.UnpauseOptions{} }
func (r *UnpauseREST) Destroy()              {}
func (r *UnpauseREST) NamespaceScoped() bool { return true }

func (r *UnpauseREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	bodyStruct := &v1.UnpauseOptions{}
	if obj != nil {
		var ok bool
		bodyStruct, ok = obj.(*v1.UnpauseOptions)
		if !ok {
			return nil, errors.NewBadRequest("invalid request body")
		}
	}

	vm, err := r.virtCli.VirtualMachine(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, errors.NewInternalError(err)
		}
	} else {
		if vm.Status.SnapshotInProgress != nil {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachine"), name,
				fmt.Errorf(vmSnapshotInprogress),
			)
		}
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmiNotRunning),
		)
	}
	condManager := controller.NewVirtualMachineInstanceConditionManager()
	if !condManager.HasCondition(vmi, v1.VirtualMachineInstancePaused) {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmiNotPaused),
		)
	}

	dryRun := len(bodyStruct.DryRun) > 0 &&
		bodyStruct.DryRun[0] == metav1.DryRunAll
	if dryRun {
		return vmi, nil
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.UnpauseURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get unpause URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

type FreezeREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewFreezeREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *FreezeREST {
	return &FreezeREST{
		virtCli:     virtCli,
		connFactory: connFactory,
	}
}

var _ k8srest.NamedCreater = &FreezeREST{}
var _ k8srest.Scoper = &FreezeREST{}
var _ k8srest.Storage = &FreezeREST{}

func (r *FreezeREST) New() runtime.Object   { return &metav1.CreateOptions{} }
func (r *FreezeREST) Destroy()              {}
func (r *FreezeREST) NamespaceScoped() bool { return true }

func (r *FreezeREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmNotRunning),
		)
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.FreezeURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get freeze URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

type UnfreezeREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewUnfreezeREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *UnfreezeREST {
	return &UnfreezeREST{
		virtCli:     virtCli,
		connFactory: connFactory,
	}
}

var _ k8srest.NamedCreater = &UnfreezeREST{}
var _ k8srest.Scoper = &UnfreezeREST{}
var _ k8srest.Storage = &UnfreezeREST{}

func (r *UnfreezeREST) New() runtime.Object   { return &metav1.CreateOptions{} }
func (r *UnfreezeREST) Destroy()              {}
func (r *UnfreezeREST) NamespaceScoped() bool { return true }

func (r *UnfreezeREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	// NOTE: UnfreezeVMIRequestHandler uses vmiNotRunning
	//       FreezeVMIRequestHandler uses vmNotRunning
	//       different error constants — keep them separate
	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmiNotRunning),
		)
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.UnfreezeURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get unfreeze URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

// ── ResetREST ────────────────────────────────────────────────────

type ResetREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewResetREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *ResetREST {
	return &ResetREST{virtCli: virtCli, connFactory: connFactory}
}

var _ k8srest.NamedCreater = &ResetREST{}
var _ k8srest.Scoper = &ResetREST{}
var _ k8srest.Storage = &ResetREST{}

func (r *ResetREST) New() runtime.Object   { return &metav1.CreateOptions{} }
func (r *ResetREST) Destroy()              {}
func (r *ResetREST) NamespaceScoped() bool { return true }

func (r *ResetREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.ResetURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get reset URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		if vmi != nil && !vmi.IsRunning() {
			return nil, errors.NewInternalError(
				fmt.Errorf("Failed to reset non-running VMI with phase %s: %v",
					vmi.Status.Phase, err),
			)
		}
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

// ── RestartREST ──────────────────────────────────────────────────

type RestartREST struct {
	virtCli kubecli.KubevirtClient
}

func NewRestartREST(virtCli kubecli.KubevirtClient) *RestartREST {
	return &RestartREST{virtCli: virtCli}
}

var _ k8srest.NamedCreater = &RestartREST{}
var _ k8srest.Scoper = &RestartREST{}
var _ k8srest.Storage = &RestartREST{}

func (r *RestartREST) New() runtime.Object   { return &v1.RestartOptions{} }
func (r *RestartREST) Destroy()              {}
func (r *RestartREST) NamespaceScoped() bool { return true }

func (r *RestartREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	bodyStruct := &v1.RestartOptions{}
	if obj != nil {
		var ok bool
		bodyStruct, ok = obj.(*v1.RestartOptions)
		if !ok {
			return nil, errors.NewBadRequest("invalid request body")
		}
	}

	if bodyStruct.GracePeriodSeconds != nil {
		if *bodyStruct.GracePeriodSeconds > 0 {
			return nil, errors.NewBadRequest(
				"For force restart, only gracePeriod=0 is supported for now",
			)
		} else if *bodyStruct.GracePeriodSeconds < 0 {
			return nil, errors.NewBadRequest(
				"gracePeriod has to be greater or equal to 0",
			)
		}
	}

	vm, err := r.virtCli.VirtualMachine(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(v1.Resource("virtualmachines"), name)
		}
		return nil, errors.NewInternalError(err)
	}

	if controller.NewVirtualMachineConditionManager().HasConditionWithStatus(
		vm,
		v1.VirtualMachineConditionType(v1.VirtualMachineInstanceVolumesChange),
		k8sv1.ConditionTrue,
	) {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"), name,
			fmt.Errorf(volumeMigrationManualRecoveryRequiredErr),
		)
	}

	runStrategy, err := vm.RunStrategy()
	if err != nil {
		return nil, errors.NewInternalError(err)
	}
	if runStrategy == v1.RunStrategyHalted || runStrategy == v1.RunStrategyOnce {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"), name,
			fmt.Errorf("RunStrategy %v does not support manual restart requests", runStrategy),
		)
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, errors.NewInternalError(err)
		}
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"), name,
			fmt.Errorf("VM is not running: %v", v1.RunStrategyHalted),
		)
	}

	patchBytes, err := getChangeRequestJson(vm,
		v1.VirtualMachineStateChangeRequest{Action: v1.StopRequest, UID: &vmi.UID},
		v1.VirtualMachineStateChangeRequest{Action: v1.StartRequest},
	)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}

	log.Log.Object(vm).V(4).Infof(patchingVMFmt, string(patchBytes))
	_, err = r.virtCli.VirtualMachine(vm.Namespace).PatchStatus(
		ctx, vm.Name, types.JSONPatchType, patchBytes,
		metav1.PatchOptions{DryRun: bodyStruct.DryRun},
	)
	if err != nil {
		if strings.Contains(err.Error(), jsonpatchTestErr) {
			return nil, errors.NewConflict(v1.Resource("virtualmachine"), name, err)
		}
		return nil, errors.NewInternalError(err)
	}

	if bodyStruct.GracePeriodSeconds != nil && *bodyStruct.GracePeriodSeconds == 0 {
		vmiPodname, err := r.findPod(ctx, namespace, vmi)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		if vmiPodname == "" {
			return vm, nil
		}
		err = r.virtCli.CoreV1().Pods(namespace).Delete(
			ctx, vmiPodname,
			metav1.DeleteOptions{GracePeriodSeconds: pointer.P(int64(1))},
		)
		if err != nil && !errors.IsNotFound(err) {
			return nil, errors.NewInternalError(err)
		}
	}

	return vm, nil
}

func (r *RestartREST) findPod(
	ctx context.Context,
	namespace string,
	vmi *v1.VirtualMachineInstance,
) (string, error) {
	fieldSelector := fields.ParseSelectorOrDie("status.phase==" + string(k8sv1.PodRunning))
	labelSelector, err := labels.Parse(fmt.Sprintf(
		"%s=virt-launcher,%s=%s",
		v1.AppLabel, v1.CreatedByLabel, string(vmi.UID),
	))
	if err != nil {
		return "", err
	}
	podList, err := r.virtCli.CoreV1().Pods(namespace).List(
		ctx,
		metav1.ListOptions{
			FieldSelector: fieldSelector.String(),
			LabelSelector: labelSelector.String(),
		},
	)
	if err != nil {
		return "", err
	}
	switch len(podList.Items) {
	case 0:
		return "", nil
	case 1:
		return podList.Items[0].Name, nil
	default:
		if vmi.Status.MigrationState != nil && vmi.Status.MigrationState.Completed {
			for _, pod := range podList.Items {
				if pod.Name == vmi.Status.MigrationState.TargetPod {
					return pod.Name, nil
				}
			}
		}
		return podList.Items[0].Name, nil
	}
}

// ── SoftRebootREST ───────────────────────────────────────────────

type SoftRebootREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewSoftRebootREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *SoftRebootREST {
	return &SoftRebootREST{virtCli: virtCli, connFactory: connFactory}
}

var _ k8srest.NamedCreater = &SoftRebootREST{}
var _ k8srest.Scoper = &SoftRebootREST{}
var _ k8srest.Storage = &SoftRebootREST{}

func (r *SoftRebootREST) New() runtime.Object   { return &metav1.CreateOptions{} }
func (r *SoftRebootREST) Destroy()              {}
func (r *SoftRebootREST) NamespaceScoped() bool { return true }

func (r *SoftRebootREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmNotRunning),
		)
	}
	condManager := controller.NewVirtualMachineInstanceConditionManager()
	if condManager.HasConditionWithStatus(vmi, v1.VirtualMachineInstancePaused, k8sv1.ConditionTrue) {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf("VMI is paused"),
		)
	}
	if !condManager.HasCondition(vmi, v1.VirtualMachineInstanceAgentConnected) {
		if features := vmi.Spec.Domain.Features; features != nil &&
			features.ACPI.Enabled != nil && !(*features.ACPI.Enabled) {
			return nil, errors.NewConflict(
				v1.Resource("virtualmachineinstance"), name,
				fmt.Errorf("VMI neither have the agent connected nor the ACPI feature enabled"),
			)
		}
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.SoftRebootURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get soft reboot URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

// ── MigrateREST ──────────────────────────────────────────────────

type MigrateREST struct {
	virtCli kubecli.KubevirtClient
}

func NewMigrateREST(virtCli kubecli.KubevirtClient) *MigrateREST {
	return &MigrateREST{virtCli: virtCli}
}

var _ k8srest.NamedCreater = &MigrateREST{}
var _ k8srest.Scoper = &MigrateREST{}
var _ k8srest.Storage = &MigrateREST{}

func (r *MigrateREST) New() runtime.Object   { return &v1.MigrateOptions{} }
func (r *MigrateREST) Destroy()              {}
func (r *MigrateREST) NamespaceScoped() bool { return true }

func (r *MigrateREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	bodyStruct := &v1.MigrateOptions{}
	if obj != nil {
		var ok bool
		bodyStruct, ok = obj.(*v1.MigrateOptions)
		if !ok {
			return nil, errors.NewBadRequest("invalid request body")
		}
	}

	_, err := r.virtCli.VirtualMachine(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(v1.Resource("virtualmachines"), name)
		}
		return nil, errors.NewInternalError(err)
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachine"), name,
			fmt.Errorf(vmNotRunning),
		)
	}

	_, err = r.virtCli.VirtualMachineInstanceMigration(namespace).Create(
		ctx,
		&v1.VirtualMachineInstanceMigration{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "kubevirt-migrate-vm-",
			},
			Spec: v1.VirtualMachineInstanceMigrationSpec{
				VMIName:           name,
				AddedNodeSelector: bodyStruct.AddedNodeSelector,
			},
		},
		metav1.CreateOptions{DryRun: bodyStruct.DryRun},
	)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

// ── BackupREST ───────────────────────────────────────────────────

type BackupREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewBackupREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *BackupREST {
	return &BackupREST{virtCli: virtCli, connFactory: connFactory}
}

var _ k8srest.NamedCreater = &BackupREST{}
var _ k8srest.Scoper = &BackupREST{}
var _ k8srest.Storage = &BackupREST{}

func (r *BackupREST) New() runtime.Object   { return &metav1.CreateOptions{} }
func (r *BackupREST) Destroy()              {}
func (r *BackupREST) NamespaceScoped() bool { return true }

func (r *BackupREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.Phase != v1.Running {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf(vmNotRunning),
		)
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.BackupURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get backup URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

// ── RedefineCheckpointREST ───────────────────────────────────────

type RedefineCheckpointREST struct {
	virtCli     kubecli.KubevirtClient
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error)
}

func NewRedefineCheckpointREST(
	virtCli kubecli.KubevirtClient,
	connFactory func(*v1.VirtualMachineInstance) (kubecli.VirtHandlerConn, error),
) *RedefineCheckpointREST {
	return &RedefineCheckpointREST{virtCli: virtCli, connFactory: connFactory}
}

var _ k8srest.NamedCreater = &RedefineCheckpointREST{}
var _ k8srest.Scoper = &RedefineCheckpointREST{}
var _ k8srest.Storage = &RedefineCheckpointREST{}

func (r *RedefineCheckpointREST) New() runtime.Object   { return &metav1.CreateOptions{} }
func (r *RedefineCheckpointREST) Destroy()              {}
func (r *RedefineCheckpointREST) NamespaceScoped() bool { return true }

func (r *RedefineCheckpointREST) Create(
	ctx context.Context,
	name string,
	obj runtime.Object,
	createValidation k8srest.ValidateObjectFunc,
	opts *metav1.CreateOptions,
) (runtime.Object, error) {

	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok {
		return nil, errors.NewBadRequest("namespace is required")
	}

	vmi, err := r.virtCli.VirtualMachineInstance(namespace).Get(
		ctx, name, metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, errors.NewNotFound(
				v1.Resource("virtualmachineinstances"), name,
			)
		}
		return nil, errors.NewInternalError(err)
	}

	if vmi.Status.ChangedBlockTracking == nil ||
		vmi.Status.ChangedBlockTracking.State != v1.ChangedBlockTrackingEnabled {
		return nil, errors.NewConflict(
			v1.Resource("virtualmachineinstance"), name,
			fmt.Errorf("ChangedBlockTracking is not enabled"),
		)
	}

	conn, err := r.connFactory(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to connect to virt-handler: %v", err),
		)
	}

	url, err := conn.RedefineCheckpointURI(vmi)
	if err != nil {
		return nil, errors.NewBadRequest(
			fmt.Sprintf("unable to get redefine checkpoint URI: %v", err),
		)
	}

	if err := conn.Put(url, nil); err != nil {
		return nil, errors.NewInternalError(err)
	}

	return vmi, nil
}

// ── Helper functions ─────────────────────────────────────────────
// These were previously in subresource.go but need to be
// defined in this file since they are used here

func getChangeRequestJson(vm *v1.VirtualMachine, changes ...v1.VirtualMachineStateChangeRequest) ([]byte, error) {
	patchSet := patch.New()
	newStatus := v1.VirtualMachineStatus{}
	if equality.Semantic.DeepEqual(vm.Status, newStatus) {
		newStatus.StateChangeRequests = append(newStatus.StateChangeRequests, changes...)
		patchSet.AddOption(patch.WithAdd("/status", newStatus))
	} else {
		patchSet.AddOption(patch.WithTest("/status/stateChangeRequests", vm.Status.StateChangeRequests))
		switch {
		case len(vm.Status.StateChangeRequests) == 0:
			patchSet.AddOption(patch.WithAdd("/status/stateChangeRequests", changes))
		case len(changes) == 1 && changes[0].Action == v1.StopRequest:
			patchSet.AddOption(patch.WithReplace("/status/stateChangeRequests", changes))
		default:
			return nil, fmt.Errorf("unable to complete request: stop/start already underway")
		}
	}
	if vm.Status.StartFailure != nil {
		patchSet.AddOption(patch.WithRemove("/status/startFailure"))
	}
	return patchSet.GeneratePayload()
}

func getRunningPatch(vm *v1.VirtualMachine, running bool) ([]byte, error) {
	runStrategy := v1.RunStrategyHalted
	if running {
		runStrategy = v1.RunStrategyAlways
	}
	if vm.Spec.RunStrategy != nil {
		return patch.New(
			patch.WithTest("/spec/runStrategy", vm.Spec.RunStrategy),
			patch.WithReplace("/spec/runStrategy", runStrategy),
		).GeneratePayload()
	}
	return patch.New(
		patch.WithTest("/spec/running", vm.Spec.Running),
		patch.WithReplace("/spec/running", running),
	).GeneratePayload()
}
