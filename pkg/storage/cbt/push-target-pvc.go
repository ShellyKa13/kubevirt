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

package cbt

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/apimachinery/patch"
	"kubevirt.io/kubevirt/pkg/pointer"
	"kubevirt.io/kubevirt/pkg/storage/types"
)

const (
	backupTargetPVC = "backup-target-pvc"
)

var (
	failedTargetPVCAttach = "Failed to attach target backup pvc: %s"
	failedTargetPVCDetach = "Failed to detach target backup pvc: %s"
	attachTargetPVCMsg    = "Attaching backup target pvc %s to vmi %s"
	detachTargetPVCMsg    = "Detaching backup target pvc %s from vmi %s"
)

func (ctrl *VMBackupController) verifyBackupTargetPVC(pvcName *string, namespace string) *SyncInfo {
	objKey := cacheKeyFunc(namespace, *pvcName)
	obj, exists, err := ctrl.pvcStore.GetByKey(objKey)
	if err != nil {
		return syncInfoError(err)
	}

	if !exists {
		return &SyncInfo{
			event:  backupTargetPVCDoesntExist,
			reason: fmt.Sprintf("PVC %s/%s doesnt exist", namespace, *pvcName),
		}
	}
	pvc := obj.(*corev1.PersistentVolumeClaim)
	if types.IsPVCBlock(pvc.Spec.VolumeMode) {
		return syncInfoError(fmt.Errorf("backup target PVC must be a filesystem PVC, provided pvc %s/%s is block", namespace, *pvcName))
	}

	return nil
}

func (ctrl *VMBackupController) backupTargetPVCAttached(vmi *v1.VirtualMachineInstance) bool {
	if vmi == nil {
		return false
	}
	for _, volumeStatus := range vmi.Status.VolumeStatus {
		if volumeStatus.Name == backupTargetPVC {
			return volumeStatus.HotplugVolume != nil && volumeStatus.Phase == v1.HotplugVolumeMounted
		}
	}
	return false
}

func (ctrl *VMBackupController) backupTargetPVCDetached(vmi *v1.VirtualMachineInstance, pvcName *string) bool {
	if vmi == nil || pvcName == nil || *pvcName == "" {
		return true
	}
	for _, volumeStatus := range vmi.Status.VolumeStatus {
		if volumeStatus.Name == *pvcName {
			return false
		}
	}
	return true
}

func (ctrl *VMBackupController) attachBackupTargetPVC(vmi *v1.VirtualMachineInstance, pvcName string) *SyncInfo {
	backupVolume := v1.UtilityVolume{
		Name: backupTargetPVC,
		PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
			ClaimName: pvcName,
		},
		Type: pointer.P(v1.Backup),
	}

	patchSet := patch.New(
		patch.WithTest("/spec/utilityVolumes", vmi.Spec.UtilityVolumes),
	)

	newUtilityVolumes := append(vmi.Spec.UtilityVolumes, backupVolume)
	if len(vmi.Spec.UtilityVolumes) > 0 {
		patchSet.AddOption(patch.WithReplace("/spec/utilityVolumes", newUtilityVolumes))
	} else {
		patchSet.AddOption(patch.WithAdd("/spec/utilityVolumes", newUtilityVolumes))
	}

	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		return syncInfoError(err)
	}

	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCAttach, err)
		log.Log.Object(vmi).Error(failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	pvcAttachMsg := fmt.Sprintf(attachTargetPVCMsg, pvcName, vmi.Name)
	log.Log.Object(vmi).Info(pvcAttachMsg)

	return &SyncInfo{
		event:  backupTargetPVCAddVolumeSubmitted,
		reason: pvcAttachMsg,
	}
}

func (ctrl *VMBackupController) detachBackupTargetPVC(vmi *v1.VirtualMachineInstance, pvcName string) *SyncInfo {
	if len(vmi.Spec.UtilityVolumes) == 0 {
		return nil
	}

	newUtilityVolumes := make([]v1.UtilityVolume, 0, len(vmi.Spec.UtilityVolumes))
	for _, vol := range vmi.Spec.UtilityVolumes {
		if vol.Name != backupTargetPVC {
			newUtilityVolumes = append(newUtilityVolumes, vol)
		}
	}

	patchSet := patch.New(
		patch.WithTest("/spec/utilityVolumes", vmi.Spec.UtilityVolumes),
	)
	if len(newUtilityVolumes) == 0 {
		patchSet.AddOption(patch.WithRemove("/spec/utilityVolumes"))
	} else {
		patchSet.AddOption(patch.WithReplace("/spec/utilityVolumes", newUtilityVolumes))
	}

	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCDetach, err)
		log.Log.Object(vmi).Error(failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCDetach, err)
		log.Log.Object(vmi).Error(failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	pvcDetachMsg := fmt.Sprintf(detachTargetPVCMsg, pvcName, vmi.Name)
	log.Log.Object(vmi).Info(pvcDetachMsg)

	return &SyncInfo{
		event:  backupTargetPVCRemoveVolumeSubmitted,
		reason: pvcDetachMsg,
	}
}
