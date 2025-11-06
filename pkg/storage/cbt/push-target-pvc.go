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
	if pvcName == nil {
		log.Log.Errorf("[BACKUP-DEBUG] verifyBackupTargetPVC: pvcName is nil")
		return syncInfoError(fmt.Errorf("backup target PVC name is nil"))
	}
	objKey := cacheKeyFunc(namespace, *pvcName)
	log.Log.Infof("[BACKUP-DEBUG] verifyBackupTargetPVC: looking for PVC %s", objKey)
	obj, exists, err := ctrl.pvcStore.GetByKey(objKey)
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Error getting PVC from store: %v", err)
		return syncInfoError(err)
	}

	if !exists {
		log.Log.Errorf("[BACKUP-DEBUG] PVC %s/%s doesn't exist", namespace, *pvcName)
		return &SyncInfo{
			event:  backupTargetPVCDoesntExist,
			reason: fmt.Sprintf("PVC %s/%s doesnt exist", namespace, *pvcName),
		}
	}
	pvc := obj.(*corev1.PersistentVolumeClaim)
	log.Log.Infof("[BACKUP-DEBUG] PVC %s exists, volumeMode: %v", objKey, pvc.Spec.VolumeMode)
	if types.IsPVCBlock(pvc.Spec.VolumeMode) {
		log.Log.Errorf("[BACKUP-DEBUG] PVC %s/%s is block mode, must be filesystem", namespace, *pvcName)
		return syncInfoError(fmt.Errorf("backup target PVC must be a filesystem PVC, provided pvc %s/%s is block", namespace, *pvcName))
	}

	log.Log.Infof("[BACKUP-DEBUG] verifyBackupTargetPVC successful for %s", objKey)
	return nil
}

func (ctrl *VMBackupController) backupTargetPVCAttached(vmi *v1.VirtualMachineInstance) bool {
	if vmi == nil {
		log.Log.Infof("[BACKUP-DEBUG] backupTargetPVCAttached: vmi is nil")
		return false
	}
	log.Log.Infof("[BACKUP-DEBUG] backupTargetPVCAttached: checking VMI %s/%s volume statuses (count: %d)",
		vmi.Namespace, vmi.Name, len(vmi.Status.VolumeStatus))
	for _, volumeStatus := range vmi.Status.VolumeStatus {
		log.Log.Infof("[BACKUP-DEBUG]   Volume: %s, Phase: %s, HotplugVolume: %v",
			volumeStatus.Name, volumeStatus.Phase, volumeStatus.HotplugVolume != nil)
		if volumeStatus.Name == backupTargetPVC {
			attached := volumeStatus.HotplugVolume != nil && volumeStatus.Phase == v1.HotplugVolumeMounted
			log.Log.Infof("[BACKUP-DEBUG] Found backup-target-pvc volume, attached: %v (phase: %s)", attached, volumeStatus.Phase)
			return attached
		}
	}
	log.Log.Infof("[BACKUP-DEBUG] backup-target-pvc volume not found in VMI volume statuses")
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
	log.Log.Infof("[BACKUP-DEBUG] attachBackupTargetPVC: attaching PVC %s to VMI %s/%s",
		pvcName, vmi.Namespace, vmi.Name)
	log.Log.Infof("[BACKUP-DEBUG] Current utilityVolumes count: %d", len(vmi.Spec.UtilityVolumes))

	// Check if we already patched the VMI with the utilityVolume
	for _, vol := range vmi.Spec.UtilityVolumes {
		if vol.Name == backupTargetPVC {
			log.Log.Infof("[BACKUP-DEBUG] backup-target-pvc already exists in utilityVolumes, not adding again")
			return nil
		}
	}

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
		log.Log.Infof("[BACKUP-DEBUG] Replacing existing utilityVolumes")
		patchSet.AddOption(patch.WithReplace("/spec/utilityVolumes", newUtilityVolumes))
	} else {
		log.Log.Infof("[BACKUP-DEBUG] Adding new utilityVolumes")
		patchSet.AddOption(patch.WithAdd("/spec/utilityVolumes", newUtilityVolumes))
	}

	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Failed to generate patch: %v", err)
		return syncInfoError(err)
	}

	log.Log.Infof("[BACKUP-DEBUG] Patching VMI with: %s", string(patchBytes))
	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCAttach, err)
		log.Log.Object(vmi).Errorf("[BACKUP-DEBUG] %s", failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	pvcAttachMsg := fmt.Sprintf(attachTargetPVCMsg, pvcName, vmi.Name)
	log.Log.Object(vmi).Infof("[BACKUP-DEBUG] %s", pvcAttachMsg)

	return &SyncInfo{
		event:  backupTargetPVCAddVolumeSubmitted,
		reason: pvcAttachMsg,
	}
}

func (ctrl *VMBackupController) detachBackupTargetPVC(vmi *v1.VirtualMachineInstance, pvcName string) *SyncInfo {
	log.Log.Infof("[BACKUP-DEBUG] detachBackupTargetPVC: detaching PVC %s from VMI %s/%s",
		pvcName, vmi.Namespace, vmi.Name)
	if len(vmi.Spec.UtilityVolumes) == 0 {
		log.Log.Infof("[BACKUP-DEBUG] No utilityVolumes to detach, already clean")
		return nil
	}

	log.Log.Infof("[BACKUP-DEBUG] Current utilityVolumes count: %d", len(vmi.Spec.UtilityVolumes))
	newUtilityVolumes := make([]v1.UtilityVolume, 0, len(vmi.Spec.UtilityVolumes))
	for _, vol := range vmi.Spec.UtilityVolumes {
		if vol.Name != backupTargetPVC {
			newUtilityVolumes = append(newUtilityVolumes, vol)
		} else {
			log.Log.Infof("[BACKUP-DEBUG] Found and removing backup-target-pvc volume")
		}
	}

	patchSet := patch.New(
		patch.WithTest("/spec/utilityVolumes", vmi.Spec.UtilityVolumes),
	)
	if len(newUtilityVolumes) == 0 {
		log.Log.Infof("[BACKUP-DEBUG] Removing utilityVolumes entirely")
		patchSet.AddOption(patch.WithRemove("/spec/utilityVolumes"))
	} else {
		log.Log.Infof("[BACKUP-DEBUG] Replacing utilityVolumes with %d volumes", len(newUtilityVolumes))
		patchSet.AddOption(patch.WithReplace("/spec/utilityVolumes", newUtilityVolumes))
	}

	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCDetach, err)
		log.Log.Object(vmi).Errorf("[BACKUP-DEBUG] Failed to generate patch: %s", failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	log.Log.Infof("[BACKUP-DEBUG] Patching VMI with: %s", string(patchBytes))
	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCDetach, err)
		log.Log.Object(vmi).Errorf("[BACKUP-DEBUG] Failed to patch VMI: %s", failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	pvcDetachMsg := fmt.Sprintf(detachTargetPVCMsg, pvcName, vmi.Name)
	log.Log.Object(vmi).Infof("[BACKUP-DEBUG] %s", pvcDetachMsg)

	return &SyncInfo{
		event:  backupTargetPVCRemoveVolumeSubmitted,
		reason: pvcDetachMsg,
	}
}
