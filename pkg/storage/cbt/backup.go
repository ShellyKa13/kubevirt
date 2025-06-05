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
	"time"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/apimachinery/patch"
	"kubevirt.io/kubevirt/pkg/controller"
	hotplugdisk "kubevirt.io/kubevirt/pkg/hotplug-disk"
	"kubevirt.io/kubevirt/pkg/pointer"
	"kubevirt.io/kubevirt/pkg/storage/status"
	"kubevirt.io/kubevirt/pkg/storage/types"
)

const (
	vmBackupFinalizer = "backup.kubevirt.io/vmbackup-protection"

	backupInitiatedEvent            = "VirtualMachineBackupInitiated"
	backupAbortedEvent              = "VirtualMachineBackupAborted"
	backupFailedEvent               = "VirtualMachineBackupFailed"
	backupCompletedWithWarningEvent = "VirtualMachineBackupCompletedWithWarning"
	backupCompletedEvent            = "VirtualMachineBackupCompletedSuccessfully"

	backupTargetPVCDoesntExist           = "BackupTargetPVCDoesntExist"
	backupSourceDoesntExist              = "BackupSourceDoesntExist"
	backupSourceNotRunning               = "BackupSourceNotRunning"
	backupSourceNoVolumesToBackup        = "BackupSourceNoVolumesToBackup"
	backupTargetPVCAddVolumeSubmitted    = "BackupTargetPVCAddVolumeSubmitted"
	backupTargetPVCRemoveVolumeSubmitted = "BackupTargetPVCRemoveVolumeSubmitted"

	backupInitializing             = "Backup is initializing"
	backupInProgress               = "Backup is in progress"
	backupDeletionBeforeCompletion = "Backup was deleted before it completed"
	backupDeleting                 = "Backup is deleting"
)

var (
	otherBackupInProgress         = "Another backup %s is already in progress"
	failedTargetPVCAttach         = "Failed to attach target backup pvc: %s"
	failedTargetPVCDetach         = "Failed to detach target backup pvc: %s"
	attachTargetPVCMsg            = "Attaching backup target pvc %s to vmi %s"
	detachTargetPVCMsg            = "Detached backup target pvc %s from vmi %s"
	backupCompletedMsg            = "Successfully completed VirtualMachineBackup"
	backupCompletedWithWarningMsg = "Completed VirtualMachineBackup, warning: %s"
)

type VMBackupController struct {
	client              kubecli.KubevirtClient
	backupInformer      cache.SharedIndexInformer
	vmStore             cache.Store
	vmiStore            cache.Store
	pvcStore            cache.Store
	recorder            record.EventRecorder
	backupQueue         workqueue.TypedRateLimitingInterface[string]
	backupStatusUpdater *status.BackupStatusUpdater
	hasSynced           func() bool
}

func NewVMBackupController(client kubecli.KubevirtClient,
	backupInformer cache.SharedIndexInformer,
	vmInformer cache.SharedIndexInformer,
	vmiInformer cache.SharedIndexInformer,
	pvcInformer cache.SharedIndexInformer,
	recorder record.EventRecorder,
) (*VMBackupController, error) {
	c := &VMBackupController{
		backupQueue: workqueue.NewTypedRateLimitingQueueWithConfig[string](
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "virt-controller-vmbackup"},
		),
		backupInformer: backupInformer,
		vmStore:        vmInformer.GetStore(),
		vmiStore:       vmiInformer.GetStore(),
		pvcStore:       pvcInformer.GetStore(),
		recorder:       recorder,
		client:         client,
	}

	c.hasSynced = func() bool {
		return backupInformer.HasSynced() && vmInformer.HasSynced() && vmiInformer.HasSynced() && pvcInformer.HasSynced()
	}

	_, err := backupInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.handleBackup,
			UpdateFunc: func(oldObj, newObj interface{}) { c.handleBackup(newObj) },
			DeleteFunc: c.handleBackup,
		},
	)
	_, err = vmiInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			UpdateFunc: c.handleUpdateVMI,
		},
	)
	if err != nil {
		return nil, err
	}

	c.backupStatusUpdater = status.NewBackupStatusUpdater(c.client)
	return c, nil
}

func (ctrl *VMBackupController) handleBackup(obj interface{}) {
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}

	if backup, ok := obj.(*backupv1.VirtualMachineBackup); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(backup)
		if err != nil {
			log.Log.Errorf("failed to get key from object: %v, %v", err, backup)
			return
		}

		log.Log.V(3).Infof("enqueued %q for sync", objName)
		ctrl.backupQueue.Add(objName)
	}
}

func cacheKeyFunc(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func (ctrl *VMBackupController) handleUpdateVMI(oldObj, newObj interface{}) {
	ovmi, ok := oldObj.(*v1.VirtualMachineInstance)
	if !ok {
		return
	}

	nvmi, ok := newObj.(*v1.VirtualMachineInstance)
	if !ok {
		return
	}

	if equality.Semantic.DeepEqual(ovmi.Status, nvmi.Status) {
		return
	}
	key := cacheKeyFunc(nvmi.Namespace, nvmi.Name)
	keys, err := ctrl.backupInformer.GetIndexer().IndexKeys("vmi", key)
	if err != nil {
		return
	}

	for _, key := range keys {
		ctrl.backupQueue.Add(key)
	}
}

func (ctrl *VMBackupController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer ctrl.backupQueue.ShutDown()

	log.Log.Info("Starting backup controller.")
	defer log.Log.Info("Shutting down backup controller.")

	if !cache.WaitForCacheSync(
		stopCh,
		ctrl.hasSynced,
	) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(ctrl.runWorker, time.Second, stopCh)
	}

	<-stopCh

	return nil
}

func (ctrl *VMBackupController) runWorker() {
	for ctrl.Execute() {
	}
}

func (ctrl *VMBackupController) Execute() bool {
	key, quit := ctrl.backupQueue.Get()
	if quit {
		return false
	}
	defer ctrl.backupQueue.Done(key)

	err := ctrl.execute(key)
	if err != nil {
		log.Log.Reason(err).Infof("reenqueuing VirtualMachineBackup %v", key)
		ctrl.backupQueue.AddRateLimited(key)
	} else {
		log.Log.V(4).Infof("processed VirtualMachineBackup %v", key)
		ctrl.backupQueue.Forget(key)
	}
	return true
}

type SyncInfo struct {
	err            error
	reason         string
	event          string
	checkpointName string
}

func syncInfoError(err error) *SyncInfo {
	return &SyncInfo{err: err}
}

func (ctrl *VMBackupController) execute(key string) error {
	logger := log.Log.With("VirtualMachineBackup", key)
	logger.Info("Execute")
	storeObj, exists, err := ctrl.backupInformer.GetStore().GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	backup, ok := storeObj.(*backupv1.VirtualMachineBackup)
	if !ok {
		return fmt.Errorf("unexpected resource %+v", storeObj)
	}

	logger.Info("before sync")
	syncInfo := ctrl.sync(backup)
	if syncInfo != nil && syncInfo.err != nil {
		logger.Infof("sync error :%+v", syncInfo.err)
		return syncInfo.err
	}

	err = ctrl.updateStatus(backup, syncInfo, logger)
	if err != nil {
		logger.Reason(err).Error("Updating the VirtualMachineBackup status failed.")
		return err
	}

	return nil
}

func (ctrl *VMBackupController) sync(backup *backupv1.VirtualMachineBackup) *SyncInfo {
	vmi, syncInfo := ctrl.verifyBackupSource(backup)
	if syncInfo != nil && syncInfo.err != nil {
		return syncInfo
	}

	if !isBackupInitializing(backup.Status) || isBackupDeleting(backup) {
		return ctrl.checkBackupCompletion(backup, vmi)
	}
	if backup.Status != nil {
		log.Log.Infof("backup conditions: %+v", backup.Status.Conditions)
	}

	backup, err := ctrl.addBackupFinalizer(backup)
	if err != nil {
		return syncInfoError(err)
	}

	if vmi == nil {
		return syncInfo
	}

	if err := ctrl.updateSourceBackupInProgress(vmi, backup.Name); err != nil {
		return syncInfoError(err)
	}
	backupOptions := backupv1.BackupOptions{
		BackupName:      backup.Name,
		Cmd:             backupv1.Start,
		BackupStartTime: &backup.CreationTimestamp,
		SkipQuiesce:     backup.Spec.SkipQuiesce,
	}
	if backup.Spec.Mode == nil {
		backup.Spec.Mode = pointer.P(backupv1.PushMode)
	}
	switch *backup.Spec.Mode {
	case backupv1.PushMode:
		pvcName := backup.Spec.PvcName
		syncInfo = ctrl.verifyBackupTargetPVC(pvcName, backup.Namespace)
		if syncInfo != nil {
			return syncInfo
		}

		if !ctrl.backupTargetPVCAttached(vmi, *pvcName) {
			return ctrl.attachBackupTargetPVC(vmi, *pvcName)
		}
		backupOptions.Mode = backupv1.PushMode
		backupOptions.PushPath = pointer.P(hotplugdisk.GetVolumeMountDir(*pvcName))
	default:
		return syncInfoError(fmt.Errorf("Invalid backup mode"))
	}

	log.Log.Object(vmi).Info("Sending Start backup command")
	err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Backup(context.Background(), vmi.Name, &backupOptions)
	if err != nil {
		log.Log.Infof("Error sending Start backup command: %s", err)
		return syncInfoError(err)
	}
	log.Log.Object(vmi).Info("Started backup command")

	return &SyncInfo{
		event:  backupInitiatedEvent,
		reason: backupInProgress,
	}
}

func (ctrl *VMBackupController) updateStatus(backup *backupv1.VirtualMachineBackup, syncInfo *SyncInfo, logger *log.FilteredLogger) error {
	backupOut := backup.DeepCopy()

	if backup.Status == nil {
		log.Log.Info("updateStatus Initializing")
		backupOut.Status = &backupv1.VirtualMachineBackupStatus{}
		updateBackupCondition(backupOut, newInitializingCondition(corev1.ConditionTrue, backupInitializing))
		updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, backupInitializing))
	}

	if syncInfo != nil {
		switch syncInfo.event {
		case backupInitiatedEvent:
			log.Log.Info("backup updateStatus Progressing")
			removeBackupCondition(backupOut, backupv1.ConditionInitializing)
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionTrue, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionFalse, syncInfo.reason))
		case backupFailedEvent:
			log.Log.Info("backup updateStatus Failed")
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
			updateBackupCondition(backupOut, newFailureCondition(corev1.ConditionTrue, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionTrue, syncInfo.reason))
			ctrl.recorder.Eventf(backupOut, corev1.EventTypeWarning, backupFailedEvent, syncInfo.reason)
		case backupAbortedEvent:
			log.Log.Info("backup updateStatus abort")
			if syncInfo.reason == string(backupv1.BackupAbortInProgress) {
				removeBackupCondition(backupOut, backupv1.ConditionInitializing)
				updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
				updateBackupCondition(backupOut, newFailureCondition(corev1.ConditionTrue, syncInfo.reason))
			} else if syncInfo.reason == string(backupv1.BackupAbortSucceeded) {
				updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
				updateBackupCondition(backupOut, newFailureCondition(corev1.ConditionTrue, syncInfo.reason))
				updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionTrue, syncInfo.reason))
				ctrl.recorder.Eventf(backupOut, corev1.EventTypeWarning, backupAbortedEvent, syncInfo.reason)
			}
		case backupCompletedEvent, backupCompletedWithWarningEvent:
			log.Log.Info("backup updateStatus Completed")
			if syncInfo.event == backupCompletedWithWarningEvent {
				ctrl.recorder.Eventf(backupOut, corev1.EventTypeWarning, backupCompletedWithWarningEvent, syncInfo.reason)
			} else {
				ctrl.recorder.Eventf(backupOut, corev1.EventTypeNormal, backupCompletedEvent, syncInfo.reason)
			}

			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionTrue, syncInfo.reason))
			backupOut.Status.CheckpointName = syncInfo.checkpointName
			backupOut.Status.Type = backupv1.Full
		}
	}

	if isBackupDeleting(backupOut) {
		updateBackupCondition(backupOut, newDeletingCondition(corev1.ConditionTrue, backupDeleting))
	}

	if !equality.Semantic.DeepEqual(backup, backupOut) {
		diff := cmp.Diff(backup, backupOut)
		log.Log.Infof("backups are different. Diff:\n%s", diff)
		return ctrl.backupStatusUpdater.UpdateStatus(backupOut)
	}
	return nil
}

func (ctrl *VMBackupController) addBackupFinalizer(backup *backupv1.VirtualMachineBackup) (*backupv1.VirtualMachineBackup, error) {
	if controller.HasFinalizer(backup, vmBackupFinalizer) {
		return backup, nil
	}

	cpy := backup.DeepCopy()
	controller.AddFinalizer(cpy, vmBackupFinalizer)

	patch, err := patch.New(
		patch.WithTest("/metadata/finalizers", backup.Finalizers),
		patch.WithReplace("/metadata/finalizers", cpy.Finalizers),
	).GeneratePayload()
	if err != nil {
		return backup, err
	}

	return ctrl.client.VirtualMachineBackup(cpy.Namespace).Patch(context.Background(), cpy.Name, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
}

func (ctrl *VMBackupController) removeBackupFinalizer(backup *backupv1.VirtualMachineBackup) (*backupv1.VirtualMachineBackup, error) {
	if !controller.HasFinalizer(backup, vmBackupFinalizer) {
		return backup, nil
	}

	cpy := backup.DeepCopy()
	controller.RemoveFinalizer(cpy, vmBackupFinalizer)

	patch, err := patch.New(
		patch.WithTest("/metadata/finalizers", backup.Finalizers),
		patch.WithReplace("/metadata/finalizers", cpy.Finalizers),
	).GeneratePayload()
	if err != nil {
		return backup, err
	}

	log.Log.Infof("remove backup %s finalizer", cpy.Name)
	return ctrl.client.VirtualMachineBackup(cpy.Namespace).Patch(context.Background(), cpy.Name, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
}

func getSourceName(backup *backupv1.VirtualMachineBackup) string {
	return backup.Spec.Source.Name
}

func (ctrl *VMBackupController) verifyBackupSource(backup *backupv1.VirtualMachineBackup) (*v1.VirtualMachineInstance, *SyncInfo) {
	sourceName := getSourceName(backup)
	objKey := cacheKeyFunc(backup.Namespace, sourceName)
	_, exists, err := ctrl.vmStore.GetByKey(objKey)
	if err != nil {
		return nil, syncInfoError(err)
	}

	if !exists {
		return nil, &SyncInfo{
			event:  backupSourceDoesntExist,
			reason: fmt.Sprintf("VM %s/%s doesnt exist", backup.Namespace, sourceName),
		}
	}
	obj, exists, err := ctrl.vmiStore.GetByKey(objKey)
	if err != nil {
		return nil, syncInfoError(err)
	}

	if !exists {
		return nil, &SyncInfo{
			event:  backupSourceNotRunning,
			reason: fmt.Sprintf("vm %s is not running, can not do backup", sourceName),
		}
	}
	vmi := obj.(*v1.VirtualMachineInstance)
	if len(vmi.Spec.Volumes) == 0 {
		return nil, &SyncInfo{
			event:  backupSourceNoVolumesToBackup,
			reason: fmt.Sprintf("vm %s has no volumes to backup", sourceName),
		}

	}
	return vmi, nil
}

func (ctrl *VMBackupController) verifyBackupTargetPVC(pvcName *string, namespace string) *SyncInfo {
	if pvcName == nil {
		return syncInfoError(fmt.Errorf("backup pvcName is empty"))
	}

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
		return syncInfoError(fmt.Errorf("provided pvc %s/%s is in block mode", namespace, *pvcName))
	}

	return nil
}

func (ctrl *VMBackupController) backupTargetPVCAttached(vmi *v1.VirtualMachineInstance, pvcName string) bool {
	for _, volumeStatus := range vmi.Status.VolumeStatus {
		if volumeStatus.Name == pvcName {
			if volumeStatus.HotplugVolume != nil &&
				volumeStatus.Phase == v1.HotplugVolumeMounted {
				return true
			} else {
				return false
			}
		}
	}
	return false
}

func addTargetPVCToVMIVolumes(vmiSpec *v1.VirtualMachineInstanceSpec, pvcName string) []v1.Volume {
	scratchVolume := &v1.ScratchVolumeSource{
		PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
			PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
			Hotpluggable: true,
		},
	}

	newVolume := v1.Volume{
		Name: pvcName,
		VolumeSource: v1.VolumeSource{
			ScratchVolume: scratchVolume,
		},
	}

	newVolumes := append(vmiSpec.Volumes, newVolume)

	return newVolumes
}

func removeTargetPVCFromVMIVolumes(vmiSpec *v1.VirtualMachineInstanceSpec, pvcName string) []v1.Volume {
	vmiVolumes := []v1.Volume{}
	for _, volume := range vmiSpec.Volumes {
		if volume.Name != pvcName {
			vmiVolumes = append(vmiVolumes, volume)
		}
	}

	return vmiVolumes
}

func attachingInProgress(vmiSpec *v1.VirtualMachineInstanceSpec, pvcName string) bool {
	for _, volume := range vmiSpec.Volumes {
		if volume.Name == pvcName {
			return true
		}
	}
	return false
}

func (ctrl *VMBackupController) patchVMIVolumes(vmi *v1.VirtualMachineInstance, newVolumes []v1.Volume) error {
	patchSet := patch.New(
		patch.WithTest("/spec/volumes", vmi.Spec.Volumes),
		patch.WithReplace("/spec/volumes", newVolumes),
	)
	patchBytes, err := patchSet.GeneratePayload()
	if err != nil {
		return err
	}

	log.Log.Object(vmi).V(4).Infof("Patching VMI: %s", string(patchBytes))
	if _, err := ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{}); err != nil {
		return err
	}
	return nil
}

func (ctrl *VMBackupController) attachBackupTargetPVC(vmi *v1.VirtualMachineInstance, pvcName string) *SyncInfo {
	if attachingInProgress(&vmi.Spec, pvcName) {
		return nil
	}
	pvcAttachMsg := fmt.Sprintf(attachTargetPVCMsg, pvcName, vmi.Name)
	log.Log.Object(vmi).Info(pvcAttachMsg)
	newVolumes := addTargetPVCToVMIVolumes(&vmi.Spec, pvcName)
	err := ctrl.patchVMIVolumes(vmi, newVolumes)
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCAttach, err)
		log.Log.Object(vmi).Error(failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	return &SyncInfo{
		event:  backupTargetPVCAddVolumeSubmitted,
		reason: pvcAttachMsg,
	}
}

func (ctrl *VMBackupController) detachBackupTargetPVC(vmi *v1.VirtualMachineInstance, pvcName string) *SyncInfo {
	if !attachingInProgress(&vmi.Spec, pvcName) {
		return nil
	}
	pvcDetachMsg := fmt.Sprintf(detachTargetPVCMsg, pvcName, vmi.Name)
	log.Log.Object(vmi).Info(pvcDetachMsg)
	newVolumes := removeTargetPVCFromVMIVolumes(&vmi.Spec, pvcName)
	err := ctrl.patchVMIVolumes(vmi, newVolumes)
	if err != nil {
		failedPatchErr := fmt.Errorf(failedTargetPVCDetach, err)
		log.Log.Object(vmi).Error(failedPatchErr.Error())
		return syncInfoError(failedPatchErr)
	}

	return &SyncInfo{
		event:  backupTargetPVCRemoveVolumeSubmitted,
		reason: pvcDetachMsg,
	}
}

func (ctrl *VMBackupController) removeSourceBackupInProgress(vmi *v1.VirtualMachineInstance) *SyncInfo {
	vmiCopy := vmi.DeepCopy()
	if vmiCopy.Status.ChangedBlockTracking == nil || vmiCopy.Status.ChangedBlockTracking.BackupStatus == nil {
		return nil
	}
	patch, err := patch.New(
		patch.WithRemove("/status/backupStatus"),
	).GeneratePayload()
	if err != nil {
		return syncInfoError(err)
	}
	log.Log.Object(vmi).V(4).Infof("Patching VMI: %s", patch)
	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		log.Log.Errorf("failed removeSourceBackupInProgress :%s", err)
		return syncInfoError(err)
	}
	log.Log.Info("removeSourceBackupInProgress updated")

	return nil
}

func (ctrl *VMBackupController) updateSourceBackupInProgress(vmi *v1.VirtualMachineInstance, backupName string) error {
	if vmi.Status.ChangedBlockTracking != nil && vmi.Status.ChangedBlockTracking.BackupStatus != nil {
		if vmi.Status.ChangedBlockTracking.BackupStatus.BackupName != backupName {
			return fmt.Errorf(otherBackupInProgress, vmi.Status.ChangedBlockTracking.BackupStatus.BackupName)
		}
		return nil
	}

	backupStatus := &v1.VirtualMachineInstanceBackupStatus{
		BackupName: backupName,
	}
	patch, err := patch.New(
		patch.WithTest("/status/backupStatus", vmi.Status.ChangedBlockTracking.BackupStatus),
		patch.WithReplace("/status/backupStatus", backupStatus),
	).GeneratePayload()
	if err != nil {
		return err
	}
	log.Log.Object(vmi).V(4).Infof("Patching VMI: %s", patch)
	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		log.Log.Errorf("failed updateSourceBackupInProgress :%s", err)
		return err
	}
	log.Log.Info("updateSourceBackupInProgress updated")

	return nil
}

func (ctrl *VMBackupController) checkBackupCompletion(backup *backupv1.VirtualMachineBackup, vmi *v1.VirtualMachineInstance) *SyncInfo {
	log.Log.Info("checkBackupCompletion")
	if isBackupDeleting(backup) {
		if syncInfo := ctrl.cleanupSource(backup, vmi); syncInfo != nil {
			return syncInfo
		}
		if _, err := ctrl.removeBackupFinalizer(backup); err != nil {
			return syncInfoError(err)
		}
		log.Log.Info("Backup got deleted before completion")
		return &SyncInfo{
			event:  backupFailedEvent,
			reason: backupDeletionBeforeCompletion,
		}
	}
	if IsBackupDone(backup.Status) {
		log.Log.Info("Backup is done")
		if syncInfo := ctrl.cleanupSource(backup, vmi); syncInfo != nil {
			return syncInfo
		}
		return nil
	}

	if vmi == nil || vmi.Status.ChangedBlockTracking == nil || vmi.Status.ChangedBlockTracking.BackupStatus == nil {
		return nil
	}
	backupStatus := vmi.Status.ChangedBlockTracking.BackupStatus
	switch {
	case backupStatus.AbortStatus != nil:
		log.Log.Infof("Backup status aborted %+v", backupStatus)
		return &SyncInfo{
			event:  backupAbortedEvent,
			reason: *backupStatus.AbortStatus,
		}
	case !backupStatus.Completed:
		return nil
	case backupStatus.Failed:
		log.Log.Infof("Backup status failed %+v", backupStatus)
		syncInfo := &SyncInfo{
			event: backupFailedEvent,
		}
		if backupStatus.BackupMsg != nil {
			syncInfo.reason = *backupStatus.BackupMsg
		}
		return syncInfo
	case backupStatus.BackupMsg != nil:
		log.Log.Infof("Backup msg %+v", backupStatus)
		return &SyncInfo{
			event:          backupCompletedWithWarningEvent,
			reason:         fmt.Sprintf(backupCompletedWithWarningMsg, *backupStatus.BackupMsg),
			checkpointName: backupStatus.CheckpointName,
		}
	}

	log.Log.Infof("Backup status completed %+v", backupStatus)
	return &SyncInfo{
		event:          backupCompletedEvent,
		reason:         backupCompletedMsg,
		checkpointName: backupStatus.CheckpointName,
	}
}

func (ctrl *VMBackupController) cleanupSource(backup *backupv1.VirtualMachineBackup, vmi *v1.VirtualMachineInstance) *SyncInfo {
	if vmi == nil {
		return nil
	}

	if vmi.Status.ChangedBlockTracking != nil && vmi.Status.ChangedBlockTracking.BackupStatus != nil && !vmi.Status.ChangedBlockTracking.BackupStatus.Completed {
		log.Log.Infof("Backup deleted before completion, aborting running backup")
		backupOptions := backupv1.BackupOptions{
			BackupName:      backup.Name,
			Cmd:             backupv1.Abort,
			BackupStartTime: &backup.CreationTimestamp,
		}
		err := ctrl.client.VirtualMachineInstance(vmi.Namespace).Backup(context.Background(), vmi.Name, &backupOptions)
		if err != nil {
			log.Log.Infof("Error sending abort backup command: %s", err)
			return syncInfoError(err)
		}
		// TODO: send abort to backup
	} else {
		if backup.Spec.Mode == nil || *backup.Spec.Mode == backupv1.PushMode {
			if ctrl.backupTargetPVCAttached(vmi, *backup.Spec.PvcName) {
				return ctrl.detachBackupTargetPVC(vmi, *backup.Spec.PvcName)
			}
		}
		if syncInfo := ctrl.removeSourceBackupInProgress(vmi); syncInfo != nil {
			return syncInfo
		}
	}

	return nil
}

func isBackupInitializing(status *backupv1.VirtualMachineBackupStatus) bool {
	return status == nil || hasCondition(status.Conditions, backupv1.ConditionInitializing)
}

func IsBackupDone(status *backupv1.VirtualMachineBackupStatus) bool {
	return status != nil && hasCondition(status.Conditions, backupv1.ConditionDone)
}

func updateCondition(conditions []backupv1.Condition, c backupv1.Condition) []backupv1.Condition {
	found := false
	for i := range conditions {
		if conditions[i].Type == c.Type {
			if conditions[i].Status != c.Status || conditions[i].Reason != c.Reason || conditions[i].Message != c.Message {
				conditions[i] = c
			}
			found = true
			break
		}
	}

	if !found {
		conditions = append(conditions, c)
	}

	return conditions
}

func newCondition(condType backupv1.ConditionType, status corev1.ConditionStatus, reason string) backupv1.Condition {
	return backupv1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		LastTransitionTime: metav1.Now(),
	}
}

func newInitializingCondition(status corev1.ConditionStatus, reason string) backupv1.Condition {
	return newCondition(backupv1.ConditionInitializing, status, reason)
}

func newDoneCondition(status corev1.ConditionStatus, reason string) backupv1.Condition {
	return newCondition(backupv1.ConditionDone, status, reason)
}

func newProgressingCondition(status corev1.ConditionStatus, reason string) backupv1.Condition {
	return newCondition(backupv1.ConditionProgressing, status, reason)
}

func newFailureCondition(status corev1.ConditionStatus, reason string) backupv1.Condition {
	return newCondition(backupv1.ConditionFailure, status, reason)
}

func newDeletingCondition(status corev1.ConditionStatus, reason string) backupv1.Condition {
	return newCondition(backupv1.ConditionDeleting, status, reason)
}

func hasCondition(conditions []backupv1.Condition, condType backupv1.ConditionType) bool {
	for _, cond := range conditions {
		if cond.Type == condType {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func updateBackupCondition(b *backupv1.VirtualMachineBackup, c backupv1.Condition) {
	b.Status.Conditions = updateCondition(b.Status.Conditions, c)
}

func removeBackupCondition(b *backupv1.VirtualMachineBackup, cType backupv1.ConditionType) {
	var conds []backupv1.Condition
	for _, c := range b.Status.Conditions {
		if c.Type == cType {
			continue
		}
		conds = append(conds, c)
	}
	b.Status.Conditions = conds
}

func isBackupDeleting(backup *backupv1.VirtualMachineBackup) bool {
	return backup != nil && backup.DeletionTimestamp != nil
}
