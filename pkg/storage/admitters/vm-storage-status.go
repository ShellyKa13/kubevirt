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

package admitters

import (
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfield "k8s.io/apimachinery/pkg/util/validation/field"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/kubevirt/pkg/controller"
	migrationutil "kubevirt.io/kubevirt/pkg/util/migrations"
	virtconfig "kubevirt.io/kubevirt/pkg/virt-config"
)

func (a *Admitter) validateRestoreStatus() []metav1.StatusCause {
	if a.ar.Operation != admissionv1.Update || a.vm.Status.RestoreInProgress == nil {
		return nil
	}

	oldVM := &v1.VirtualMachine{}
	if err := json.Unmarshal(a.ar.OldObject.Raw, oldVM); err != nil {
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeUnexpectedServerResponse,
			Message: "Could not fetch old vm",
		}}
	}

	if !equality.Semantic.DeepEqual(oldVM.Spec, a.vm.Spec) {
		oldStrategy, _ := oldVM.RunStrategy()
		newStrategy, _ := a.vm.RunStrategy()
		if newStrategy != oldStrategy {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueNotSupported,
				Message: fmt.Sprintf("Cannot update VM runStrategy until restore %q completes", *a.vm.Status.RestoreInProgress),
				Field:   k8sfield.NewPath("spec").String(),
			}}
		}
	}

	return nil
}

func (a *Admitter) validateSnapshotStatus() []metav1.StatusCause {
	if a.ar.Operation != admissionv1.Update || a.vm.Status.SnapshotInProgress == nil {
		return nil
	}

	oldVM := &v1.VirtualMachine{}
	if err := json.Unmarshal(a.ar.OldObject.Raw, oldVM); err != nil {
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeUnexpectedServerResponse,
			Message: "Could not fetch old vm",
		}}
	}

	if !compareVolumes(oldVM.Spec.Template.Spec.Volumes, a.vm.Spec.Template.Spec.Volumes) {
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeFieldValueNotSupported,
			Message: fmt.Sprintf("Cannot update vm disks or volumes until snapshot %q completes", *a.vm.Status.SnapshotInProgress),
			Field:   k8sfield.NewPath("spec").String(),
		}}
	}
	if !compareRunningSpec(&oldVM.Spec, &a.vm.Spec) {
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeFieldValueNotSupported,
			Message: fmt.Sprintf("Cannot update vm running state until snapshot %q completes", *a.vm.Status.SnapshotInProgress),
			Field:   k8sfield.NewPath("spec").String(),
		}}
	}

	return nil
}

func compareVolumes(old, new []v1.Volume) bool {
	if len(old) != len(new) {
		return false
	}

	for i, volume := range old {
		if !equality.Semantic.DeepEqual(volume, new[i]) {
			return false
		}
	}

	return true
}

func compareRunningSpec(old, new *v1.VirtualMachineSpec) bool {
	if old == nil || new == nil {
		// This should never happen, but just in case return false
		return false
	}

	// Its impossible to get here while both running and RunStrategy are nil.
	if old.Running != nil && new.Running != nil {
		return *old.Running == *new.Running
	}
	if old.RunStrategy != nil && new.RunStrategy != nil {
		return *old.RunStrategy == *new.RunStrategy
	}
	return false
}

func ValidateVMISpecStorageFields(field *k8sfield.Path, spec *v1.VirtualMachineInstanceSpec, config *virtconfig.ClusterConfig) []metav1.StatusCause {
	var causes []metav1.StatusCause
	causes = append(causes, ValidateBootOrder(field, spec, config)...)
	causes = append(causes, ValidateDisks(field.Child("domain", "devices", "disks"), spec.Domain.Devices.Disks)...)
	causes = append(causes, ValidateContainerDisks(field, spec)...)
	return causes
}

func (a *Admitter) validateVolumeRequests() []metav1.StatusCause {
	if len(a.vm.Status.VolumeRequests) == 0 {
		return nil
	}

	curVMAddRequestsMap := make(map[string]*v1.VirtualMachineVolumeRequest)
	curVMRemoveRequestsMap := make(map[string]*v1.VirtualMachineVolumeRequest)

	vmVolumeMap := make(map[string]v1.Volume)
	vmiVolumeMap := make(map[string]v1.Volume)

	vmi := &v1.VirtualMachineInstance{}
	vmiExists := false

	// get VMI if vm is active
	if a.vm.Status.Ready {
		var err error

		vmi, err = a.virtClient.VirtualMachineInstance(a.vm.Namespace).Get(a.ctx, a.vm.Name, metav1.GetOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeUnexpectedServerResponse,
				Message: fmt.Sprintf("Failed to get VirtualMachineInstance: %v", err),
				Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
			}}
		} else if err == nil && vmi.DeletionTimestamp == nil {
			// ignore validating the vmi if it is being deleted
			vmiExists = true
		}
	}

	if vmiExists {
		for _, volume := range vmi.Spec.Volumes {
			vmiVolumeMap[volume.Name] = volume
		}
	}

	for _, volume := range a.vm.Spec.Template.Spec.Volumes {
		vmVolumeMap[volume.Name] = volume
	}

	newSpec := a.vm.Spec.Template.Spec.DeepCopy()
	for _, volumeRequest := range a.vm.Status.VolumeRequests {
		volumeRequest := volumeRequest
		name := ""
		if volumeRequest.AddVolumeOptions != nil && volumeRequest.RemoveVolumeOptions != nil {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: "VolumeRequests require either addVolumeOptions or removeVolumeOptions to be set, not both",
				Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
			}}
		} else if volumeRequest.AddVolumeOptions != nil {
			name = volumeRequest.AddVolumeOptions.Name

			_, ok := curVMAddRequestsMap[name]
			if ok {
				return []metav1.StatusCause{{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("AddVolume request for [%s] aleady exists", name),
					Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
				}}
			}

			// Validate the disk is configured properly
			invalidDiskStatusCause := ValidateHotplugDiskConfiguration(
				volumeRequest.AddVolumeOptions.Disk, name,
				"AddVolume request",
				k8sfield.NewPath("Status", "volumeRequests").String(),
			)
			if invalidDiskStatusCause != nil {
				return invalidDiskStatusCause
			}

			newVolume := v1.Volume{
				Name: volumeRequest.AddVolumeOptions.Name,
			}
			if volumeRequest.AddVolumeOptions.VolumeSource.PersistentVolumeClaim != nil {
				newVolume.VolumeSource.PersistentVolumeClaim = volumeRequest.AddVolumeOptions.VolumeSource.PersistentVolumeClaim
			} else if volumeRequest.AddVolumeOptions.VolumeSource.DataVolume != nil {
				newVolume.VolumeSource.DataVolume = volumeRequest.AddVolumeOptions.VolumeSource.DataVolume
			}

			vmVolume, ok := vmVolumeMap[name]
			if ok && !equality.Semantic.DeepEqual(newVolume, vmVolume) {
				return []metav1.StatusCause{{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("AddVolume request for [%s] conflicts with an existing volume of the same name on the vmi template.", name),
					Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
				}}
			}

			vmiVolume, ok := vmiVolumeMap[name]
			if ok && !equality.Semantic.DeepEqual(newVolume, vmiVolume) {
				return []metav1.StatusCause{{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("AddVolume request for [%s] conflicts with an existing volume of the same name on currently running vmi", name),
					Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
				}}
			}

			curVMAddRequestsMap[name] = &volumeRequest
		} else if volumeRequest.RemoveVolumeOptions != nil {
			name = volumeRequest.RemoveVolumeOptions.Name

			_, ok := curVMRemoveRequestsMap[name]
			if ok {
				return []metav1.StatusCause{{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("RemoveVolume request for [%s] aleady exists", name),
					Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
				}}
			}

			curVMRemoveRequestsMap[name] = &volumeRequest
		} else {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: "VolumeRequests require one of either addVolumeOptions or removeVolumeOptions to be set",
				Field:   k8sfield.NewPath("Status", "volumeRequests").String(),
			}}
		}
		newSpec = controller.ApplyVolumeRequestOnVMISpec(newSpec, &volumeRequest)

		if vmiExists {
			vmi.Spec = *controller.ApplyVolumeRequestOnVMISpec(&vmi.Spec, &volumeRequest)
		}
	}

	// this simulates injecting the changes into the VMI template and validates it will work.
	causes := ValidateVMISpecStorageFields(k8sfield.NewPath("spec", "template", "spec"), newSpec, a.clusterConfig)
	if len(causes) > 0 {
		return causes
	}

	// This simulates injecting the changes directly into the vmi, if the vmi exists
	if vmiExists {
		causes := ValidateVMISpecStorageFields(k8sfield.NewPath("spec"), &vmi.Spec, a.clusterConfig)
		if len(causes) > 0 {
			return causes
		}

		if migrationutil.IsMigrating(vmi) {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueNotSupported,
				Message: fmt.Sprintf("Cannot handle volume requests while VMI migration is in progress"),
				Field:   k8sfield.NewPath("spec").String(),
			}}
		}
	}

	return nil
}
