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
	backupCompleted                = "Successfully completed VirtualMachineBackup"
	backupFailed                   = "Failed to create VirtualMachineBackup"
)

var (
	otherBackupInProgress         = "Another backup %s is already in progress"
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
	if err != nil {
		return nil, err
	}
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
	logger.Infof("[BACKUP-DEBUG] Processing VirtualMachineBackup %s", key)
	storeObj, exists, err := ctrl.backupInformer.GetStore().GetByKey(key)
	if err != nil {
		logger.Errorf("[BACKUP-DEBUG] Error getting backup from store: %v", err)
		return err
	}
	if !exists {
		logger.Infof("[BACKUP-DEBUG] Backup %s no longer exists in store", key)
		return nil
	}

	backup, ok := storeObj.(*backupv1.VirtualMachineBackup)
	if !ok {
		logger.Errorf("[BACKUP-DEBUG] Unexpected resource type: %T", storeObj)
		return fmt.Errorf("unexpected resource %+v", storeObj)
	}

	logger.Infof("[BACKUP-DEBUG] Calling sync for backup %s", key)
	syncInfo := ctrl.sync(backup)
	if syncInfo != nil && syncInfo.err != nil {
		logger.Infof("[BACKUP-DEBUG] sync returned error: %+v", syncInfo.err)
		return syncInfo.err
	}
	if syncInfo != nil {
		logger.Infof("[BACKUP-DEBUG] sync returned syncInfo: event=%s, reason=%s, checkpointName=%s",
			syncInfo.event, syncInfo.reason, syncInfo.checkpointName)
	} else {
		logger.Infof("[BACKUP-DEBUG] sync returned nil syncInfo")
	}

	logger.Infof("[BACKUP-DEBUG] Updating status for backup %s", key)
	err = ctrl.updateStatus(backup, syncInfo, logger)
	if err != nil {
		logger.Reason(err).Errorf("[BACKUP-DEBUG] Updating the VirtualMachineBackup status failed.")
		return err
	}

	logger.Infof("[BACKUP-DEBUG] Successfully processed backup %s", key)
	return nil
}

func (ctrl *VMBackupController) sync(backup *backupv1.VirtualMachineBackup) *SyncInfo {
	sourceName := getSourceName(backup)
	log.Log.Infof("[BACKUP-DEBUG] Starting sync for backup %s/%s, source: %s", backup.Namespace, backup.Name, sourceName)
	if sourceName == "" {
		log.Log.Errorf("[BACKUP-DEBUG] Source name is empty for backup %s/%s", backup.Namespace, backup.Name)
		return syncInfoError(fmt.Errorf("source name is empty"))
	}

	if isBackupDeleting(backup) {
		log.Log.Infof("[BACKUP-DEBUG] Backup %s/%s is being deleted", backup.Namespace, backup.Name)
		return ctrl.deletionCleanup(backup)
	}

	log.Log.Infof("[BACKUP-DEBUG] Verifying backup source for %s/%s", backup.Namespace, backup.Name)
	vmi, syncInfo := ctrl.verifyBackupSource(backup)
	if syncInfo != nil {
		log.Log.Infof("[BACKUP-DEBUG] verifyBackupSource returned syncInfo: %+v", syncInfo)
		return syncInfo
	}

	log.Log.Infof("[BACKUP-DEBUG] backup UID %s, isBackupInitializing: %v, vmi: %v", backup.UID, isBackupInitializing(backup.Status), vmi != nil)
	if !isBackupInitializing(backup.Status) || vmi == nil {
		log.Log.Infof("[BACKUP-DEBUG] Checking backup completion for %s/%s", backup.Namespace, backup.Name)
		return ctrl.checkBackupCompletion(backup, vmi)
	}
	if backup.Status != nil {
		log.Log.Infof("[BACKUP-DEBUG] backup conditions: %+v", backup.Status.Conditions)
	}

	log.Log.Infof("[BACKUP-DEBUG] Adding backup finalizer for %s/%s", backup.Namespace, backup.Name)
	backup, err := ctrl.addBackupFinalizer(backup)
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Failed to add finalizer: %v", err)
		return syncInfoError(err)
	}

	log.Log.Infof("[BACKUP-DEBUG] Updating source backup in progress for VMI %s/%s", vmi.Namespace, vmi.Name)
	if err := ctrl.updateSourceBackupInProgress(vmi, backup.Name); err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Failed to update source backup in progress: %v", err)
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
	log.Log.Infof("[BACKUP-DEBUG] Backup mode: %s", *backup.Spec.Mode)
	switch *backup.Spec.Mode {
	case backupv1.PushMode:
		pvcName := backup.Spec.PvcName
		if pvcName != nil {
			log.Log.Infof("[BACKUP-DEBUG] Push mode: verifying target PVC %s", *pvcName)
		} else {
			log.Log.Errorf("[BACKUP-DEBUG] Backup target PVC name is nil")
		}
		syncInfo = ctrl.verifyBackupTargetPVC(pvcName, backup.Namespace)
		if syncInfo != nil {
			log.Log.Infof("[BACKUP-DEBUG] verifyBackupTargetPVC returned syncInfo: %+v", syncInfo)
			return syncInfo
		}

		attached := ctrl.backupTargetPVCAttached(vmi)
		log.Log.Infof("[BACKUP-DEBUG] Backup target PVC attached: %v", attached)
		if !attached {
			pvcNameForLog := "nil"
			if pvcName != nil {
				pvcNameForLog = *pvcName
			}
			log.Log.Infof("[BACKUP-DEBUG] Attaching backup target PVC %s to VMI %s/%s", pvcNameForLog, vmi.Namespace, vmi.Name)
			return ctrl.attachBackupTargetPVC(vmi, *pvcName)
		}
		backupOptions.Mode = backupv1.PushMode
		// Use the utility volume name (backup-target-pvc), not the PVC name, for the mount path
		backupOptions.PushPath = pointer.P(hotplugdisk.GetVolumeMountDir(backupTargetPVC))
		log.Log.Infof("[BACKUP-DEBUG] Backup target PVC is attached, push path: %s (volume name: %s)", *backupOptions.PushPath, backupTargetPVC)
	default:
		log.Log.Errorf("[BACKUP-DEBUG] Invalid backup mode: %s", *backup.Spec.Mode)
		return syncInfoError(fmt.Errorf("invalid backup mode"))
	}

	log.Log.Object(vmi).Infof("[BACKUP-DEBUG] Sending Start backup command with options: %+v", backupOptions)
	err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Backup(context.Background(), vmi.Name, &backupOptions)
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Error sending Start backup command: %s", err)
		return syncInfoError(err)
	}
	log.Log.Object(vmi).Info("[BACKUP-DEBUG] Started backup command successfully")

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
		log.Log.Infof("syncInfo: %+v", syncInfo)
		switch syncInfo.event {
		case backupInitiatedEvent:
			log.Log.Info("backup updateStatus Progressing")
			removeBackupCondition(backupOut, backupv1.ConditionInitializing)
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionTrue, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionFalse, syncInfo.reason))
		case backupFailedEvent:
			log.Log.Info("backup updateStatus Failed")
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, backupFailed))
			updateBackupCondition(backupOut, newFailureCondition(corev1.ConditionTrue, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionTrue, backupFailed))
			ctrl.recorder.Eventf(backupOut, corev1.EventTypeWarning, backupFailedEvent, syncInfo.reason)
		case backupAbortedEvent:
			log.Log.Info("backup updateStatus abort")
			if syncInfo.reason == string(backupv1.BackupAbortInProgress) {
				removeBackupCondition(backupOut, backupv1.ConditionInitializing)
				updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
				updateBackupCondition(backupOut, newFailureCondition(corev1.ConditionTrue, syncInfo.reason))
				ctrl.recorder.Eventf(backupOut, corev1.EventTypeWarning, backupAbortedEvent, syncInfo.reason)
			} else if syncInfo.reason == string(backupv1.BackupAbortSucceeded) ||
				syncInfo.reason == string(backupv1.BackupAbortFailed) {
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

	if isBackupDeleting(backupOut) && controller.HasFinalizer(backupOut, vmBackupFinalizer) {
		log.Log.Infof("update backup %s is deleting", backupOut.Name)
		updateBackupCondition(backupOut, newDeletingCondition(corev1.ConditionTrue, backupDeleting))
	}

	if !equality.Semantic.DeepEqual(backup, backupOut) {
		if _, err := ctrl.client.VirtualMachineBackup(backupOut.Namespace).UpdateStatus(context.Background(), backupOut, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func generateFinalizerPatch(test, replace []string) ([]byte, error) {
	return patch.New(
		patch.WithTest("/metadata/finalizers", test),
		patch.WithReplace("/metadata/finalizers", replace),
	).GeneratePayload()
}

func (ctrl *VMBackupController) addBackupFinalizer(backup *backupv1.VirtualMachineBackup) (*backupv1.VirtualMachineBackup, error) {
	if controller.HasFinalizer(backup, vmBackupFinalizer) {
		return backup, nil
	}

	cpy := backup.DeepCopy()
	controller.AddFinalizer(cpy, vmBackupFinalizer)

	patchBytes, err := generateFinalizerPatch(backup.Finalizers, cpy.Finalizers)
	if err != nil {
		return backup, err
	}

	return ctrl.client.VirtualMachineBackup(cpy.Namespace).Patch(context.Background(), cpy.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
}

func (ctrl *VMBackupController) removeBackupFinalizer(backup *backupv1.VirtualMachineBackup) *SyncInfo {
	if !controller.HasFinalizer(backup, vmBackupFinalizer) {
		log.Log.Infof("backup %s does not have finalizer", backup.Name)
		return nil
	}
	log.Log.Infof("removing backup %s finalizer", backup.Name)

	cpy := backup.DeepCopy()
	controller.RemoveFinalizer(cpy, vmBackupFinalizer)

	patchBytes, err := generateFinalizerPatch(backup.Finalizers, cpy.Finalizers)
	if err != nil {
		return syncInfoError(fmt.Errorf("failed to remove backup %s finalizer: %w", cpy.Name, err))
	}

	backup, err = ctrl.client.VirtualMachineBackup(cpy.Namespace).Patch(context.Background(), cpy.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		return syncInfoError(fmt.Errorf("failed to remove backup %s finalizer: %w", cpy.Name, err))
	}
	log.Log.Infof("removed backup %s finalizer", backup.Name)
	return nil
}

func getSourceName(backup *backupv1.VirtualMachineBackup) string {
	// source name is fetched from the source field which is required until VirtualMachineBackupTracker is introduced
	return backup.Spec.Source.Name
}

func (ctrl *VMBackupController) getVMI(backup *backupv1.VirtualMachineBackup) (*v1.VirtualMachineInstance, bool, error) {
	sourceName := getSourceName(backup)
	objKey := cacheKeyFunc(backup.Namespace, sourceName)

	obj, exists, err := ctrl.vmiStore.GetByKey(objKey)
	if err != nil {
		return nil, false, err
	}

	if !exists {
		return nil, false, nil
	}

	return obj.(*v1.VirtualMachineInstance), exists, nil
}

func (ctrl *VMBackupController) verifyBackupSource(backup *backupv1.VirtualMachineBackup) (*v1.VirtualMachineInstance, *SyncInfo) {
	sourceName := getSourceName(backup)
	objKey := cacheKeyFunc(backup.Namespace, sourceName)
	log.Log.Infof("[BACKUP-DEBUG] verifyBackupSource: looking for VM %s", objKey)
	_, exists, err := ctrl.vmStore.GetByKey(objKey)
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Error getting VM from store: %v", err)
		return nil, syncInfoError(err)
	}

	if !exists {
		log.Log.Errorf("[BACKUP-DEBUG] VM %s/%s doesn't exist", backup.Namespace, sourceName)
		return nil, &SyncInfo{
			event:  backupSourceDoesntExist,
			reason: fmt.Sprintf("VM %s/%s doesnt exist", backup.Namespace, sourceName),
		}
	}
	log.Log.Infof("[BACKUP-DEBUG] VM exists, checking for VMI %s", objKey)
	vmi, exists, err := ctrl.getVMI(backup)
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] Error getting VMI from store: %v", err)
		return nil, syncInfoError(err)
	}
	if !exists {
		log.Log.Errorf("[BACKUP-DEBUG] VMI %s/%s doesn't exist (VM not running)", backup.Namespace, sourceName)
		return nil, &SyncInfo{
			event:  backupSourceNotRunning,
			reason: fmt.Sprintf("vm %s is not running, can not do backup", sourceName),
		}
	}
	log.Log.Infof("[BACKUP-DEBUG] VMI exists with %d volumes", len(vmi.Spec.Volumes))
	if len(vmi.Spec.Volumes) == 0 {
		log.Log.Errorf("[BACKUP-DEBUG] VMI %s/%s has no volumes to backup", backup.Namespace, sourceName)
		return nil, &SyncInfo{
			event:  backupSourceNoVolumesToBackup,
			reason: fmt.Sprintf("vm %s has no volumes to backup", sourceName),
		}

	}
	log.Log.Infof("[BACKUP-DEBUG] verifyBackupSource successful for %s/%s", backup.Namespace, sourceName)
	return vmi, nil
}

func (ctrl *VMBackupController) removeSourceBackupInProgress(vmi *v1.VirtualMachineInstance) *SyncInfo {
	if vmi == nil || vmi.Status.ChangedBlockTracking == nil || vmi.Status.ChangedBlockTracking.BackupStatus == nil {
		return nil
	}

	patch, err := patch.New(
		patch.WithRemove("/status/changedBlockTracking/backupStatus"),
	).GeneratePayload()
	if err != nil {
		return syncInfoError(err)
	}

	log.Log.Object(vmi).Infof("[BACKUP-DEBUG] Removing changedBlockTracking/backupStatus with patch: %s", patch)
	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patch, metav1.PatchOptions{})
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] failed to remove BackupInProgress from VMI %s/%s: %s", vmi.Namespace, vmi.Name, err)
		return syncInfoError(err)
	}
	log.Log.Infof("[BACKUP-DEBUG] removed BackupInProgress from VMI")

	return nil
}

func (ctrl *VMBackupController) updateSourceBackupInProgress(vmi *v1.VirtualMachineInstance, backupName string) error {
	if vmi == nil || vmi.Status.ChangedBlockTracking == nil {
		log.Log.Errorf("[BACKUP-DEBUG] vmi or vmi.Status.ChangedBlockTracking is nil, cannot update backup status")
		return fmt.Errorf("vmi or vmi.Status.ChangedBlockTracking is nil")
	}

	if vmi.Status.ChangedBlockTracking != nil && vmi.Status.ChangedBlockTracking.BackupStatus != nil {
		if vmi.Status.ChangedBlockTracking.BackupStatus.BackupName != backupName {
			return fmt.Errorf(otherBackupInProgress, vmi.Status.ChangedBlockTracking.BackupStatus.BackupName)
		}
		return nil
	}

	backupStatus := &v1.VirtualMachineInstanceBackupStatus{
		BackupName: backupName,
	}

	patchOps := patch.New(
		patch.WithTest("/status/changedBlockTracking/backupStatus", vmi.Status.ChangedBlockTracking.BackupStatus),
	)
	if vmi.Status.ChangedBlockTracking.BackupStatus == nil {
		patchOps.AddOption(patch.WithAdd("/status/changedBlockTracking/backupStatus", backupStatus))
	} else {
		patchOps.AddOption(patch.WithReplace("/status/changedBlockTracking/backupStatus", backupStatus))
	}

	patchBytes, err := patchOps.GeneratePayload()
	if err != nil {
		return err
	}
	log.Log.Object(vmi).Infof("[BACKUP-DEBUG] Patching VMI changedBlockTracking/backupStatus with: %s", patchBytes)
	_, err = ctrl.client.VirtualMachineInstance(vmi.Namespace).Patch(context.Background(), vmi.Name, k8stypes.JSONPatchType, patchBytes, metav1.PatchOptions{})
	if err != nil {
		log.Log.Errorf("[BACKUP-DEBUG] failed updateSourceBackupInProgress: %s", err)
		return err
	}
	log.Log.Infof("[BACKUP-DEBUG] updateSourceBackupInProgress updated successfully")

	return nil
}

func (ctrl *VMBackupController) checkBackupCompletion(backup *backupv1.VirtualMachineBackup, vmi *v1.VirtualMachineInstance) *SyncInfo {
	log.Log.Infof("[BACKUP-DEBUG] checkBackupCompletion for %s/%s, vmi: %v", backup.Namespace, backup.Name, vmi != nil)
	if IsBackupDone(backup.Status) {
		log.Log.Infof("[BACKUP-DEBUG] Backup %s/%s is already done", backup.Namespace, backup.Name)
		return nil
	}

	if vmi == nil || vmi.Status.ChangedBlockTracking == nil || vmi.Status.ChangedBlockTracking.BackupStatus == nil {
		log.Log.Infof("[BACKUP-DEBUG] VMI or backup status not available (vmi: %v, CBT: %v, BackupStatus: %v), starting cleanup",
			vmi != nil,
			vmi != nil && vmi.Status.ChangedBlockTracking != nil,
			vmi != nil && vmi.Status.ChangedBlockTracking != nil && vmi.Status.ChangedBlockTracking.BackupStatus != nil)
		done, syncInfo := ctrl.cleanup(backup, vmi)
		if syncInfo != nil {
			log.Log.Infof("[BACKUP-DEBUG] cleanup returned syncInfo: %+v", syncInfo)
			return syncInfo
		}
		if !done {
			log.Log.Infof("[BACKUP-DEBUG] Cleanup in progress, requeueing to wait for completion")
			return nil
		}
		log.Log.Infof("[BACKUP-DEBUG] Cleanup done but backup status still not available, returning nil")
		return nil
	}

	backupStatus := vmi.Status.ChangedBlockTracking.BackupStatus
	abortStatusStr := "nil"
	if backupStatus.AbortStatus != nil {
		abortStatusStr = *backupStatus.AbortStatus
	}
	backupMsgStr := "nil"
	if backupStatus.BackupMsg != nil {
		backupMsgStr = *backupStatus.BackupMsg
	}
	log.Log.Infof("[BACKUP-DEBUG] VMI backup status: completed=%v, failed=%v, checkpointName=%s, abortStatus=%s, backupMsg=%s",
		backupStatus.Completed, backupStatus.Failed, backupStatus.CheckpointName, abortStatusStr, backupMsgStr)
	if !backupStatus.Completed {
		log.Log.Infof("[BACKUP-DEBUG] Backup not yet completed, waiting...")
		return nil
	}

	log.Log.Infof("[BACKUP-DEBUG] Backup completed, performing cleanup before marking as done")
	done, syncInfo := ctrl.cleanup(backup, vmi)
	if syncInfo != nil {
		log.Log.Infof("[BACKUP-DEBUG] cleanup returned syncInfo: %+v", syncInfo)
		return syncInfo
	}
	if !done {
		log.Log.Infof("[BACKUP-DEBUG] Cleanup in progress, requeueing to wait for completion")
		return nil
	}

	switch {
	case backupStatus.AbortStatus != nil:
		log.Log.Infof("Backup status aborted %+v", backupStatus)
		syncInfo = &SyncInfo{
			event:  backupAbortedEvent,
			reason: *backupStatus.AbortStatus,
		}
	case backupStatus.Failed:
		log.Log.Infof("Backup status failed %+v", backupStatus)
		syncInfo = &SyncInfo{
			event: backupFailedEvent,
		}
		if backupStatus.BackupMsg != nil {
			syncInfo.reason = *backupStatus.BackupMsg
		}
	case backupStatus.BackupMsg != nil:
		log.Log.Infof("Backup msg %+v", backupStatus)
		syncInfo = &SyncInfo{
			event:          backupCompletedWithWarningEvent,
			reason:         fmt.Sprintf(backupCompletedWithWarningMsg, *backupStatus.BackupMsg),
			checkpointName: backupStatus.CheckpointName,
		}
	default:
		log.Log.Infof("Backup completed successfully %+v", backupStatus)
		syncInfo = &SyncInfo{
			event:          backupCompletedEvent,
			reason:         backupCompleted,
			checkpointName: backupStatus.CheckpointName,
		}
	}

	log.Log.Infof("Backup status completed %+v", backupStatus)
	return syncInfo
}

func backupNotCompleted(vmi *v1.VirtualMachineInstance) bool {
	return vmi != nil && vmi.Status.ChangedBlockTracking != nil && vmi.Status.ChangedBlockTracking.BackupStatus != nil && !vmi.Status.ChangedBlockTracking.BackupStatus.Completed
}

func isAbortInitiated(backup *backupv1.VirtualMachineBackup) bool {
	if backup.Status == nil || backup.Status.Conditions == nil {
		return false
	}
	for _, cond := range backup.Status.Conditions {
		if cond.Reason == string(backupv1.BackupAbortInProgress) {
			return true
		}
	}
	return false
}

func (ctrl *VMBackupController) abortBackup(backup *backupv1.VirtualMachineBackup, vmi *v1.VirtualMachineInstance) *SyncInfo {
	if vmi == nil {
		log.Log.Infof("VMI is nil, cannot send abort command for backup %s but marking as aborted", backup.Name)
		return &SyncInfo{
			event:  backupAbortedEvent,
			reason: "Backup aborted due to deletion before completion (VMI no longer exists)",
		}
	}

	backupStatus := vmi.Status.ChangedBlockTracking
	if backupStatus == nil || backupStatus.BackupStatus == nil {
		return nil
	}

	// Check if abort is already in progress or completed
	if backupStatus.BackupStatus.AbortStatus != nil {
		switch *backupStatus.BackupStatus.AbortStatus {
		case string(backupv1.BackupAbortInProgress):
			log.Log.V(3).Infof("Backup abort is in progress, waiting for completion")
			return nil
		case string(backupv1.BackupAbortSucceeded):
			log.Log.Infof("Backup abort succeeded")
			return &SyncInfo{
				event:  backupAbortedEvent,
				reason: string(backupv1.BackupAbortSucceeded),
			}
		case string(backupv1.BackupAbortFailed):
			log.Log.Warningf("Backup abort failed")
			return &SyncInfo{
				event:  backupAbortedEvent,
				reason: string(backupv1.BackupAbortFailed),
			}
		}
	}

	// Check if backup is still in progress and needs to be aborted
	if !backupStatus.BackupStatus.Completed {
		if isAbortInitiated(backup) {
			log.Log.V(3).Infof("Abort already initiated for backup %s, waiting for status update", backup.Name)
			return nil
		}

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
		log.Log.Infof("Abort command sent for backup %s, updating backup status", backup.Name)
		return &SyncInfo{
			event:  backupAbortedEvent,
			reason: string(backupv1.BackupAbortInProgress),
		}
	}

	return nil
}

func (ctrl *VMBackupController) deletionCleanup(backup *backupv1.VirtualMachineBackup) *SyncInfo {
	vmi, _, err := ctrl.getVMI(backup)
	if err != nil {
		return syncInfoError(err)
	}
	// Check if backup is still in progress and needs to be aborted
	if backupNotCompleted(vmi) && !IsBackupDone(backup.Status) {
		log.Log.Warningf("Backup deleted before completion, aborting running backup %s", backup.Name)
		syncInfo := ctrl.abortBackup(backup, vmi)
		if syncInfo != nil {
			return syncInfo
		}
		return nil
	}
	_, syncInfo := ctrl.cleanup(backup, vmi)
	if syncInfo != nil {
		return syncInfo
	}
	return nil
}

func isPushMode(backup *backupv1.VirtualMachineBackup) bool {
	return backup.Spec.Mode == nil || *backup.Spec.Mode == backupv1.PushMode
}

func (ctrl *VMBackupController) cleanup(backup *backupv1.VirtualMachineBackup, vmi *v1.VirtualMachineInstance) (bool, *SyncInfo) {
	log.Log.Infof("[BACKUP-DEBUG] cleanup: starting cleanup for backup %s/%s", backup.Namespace, backup.Name)
	if isPushMode(backup) {
		detached := ctrl.backupTargetPVCDetached(vmi, backup.Spec.PvcName)
		log.Log.Infof("[BACKUP-DEBUG] cleanup: push mode, PVC detached: %v", detached)
		if !detached {
			pvcNameStr := "nil"
			if backup.Spec.PvcName != nil {
				pvcNameStr = *backup.Spec.PvcName
			}
			log.Log.Infof("[BACKUP-DEBUG] cleanup: detaching backup target PVC %s", pvcNameStr)
			return false, ctrl.detachBackupTargetPVC(vmi, *backup.Spec.PvcName)
		}
	}

	if isBackupDeleting(backup) || IsBackupDone(backup.Status) {
		log.Log.Infof("[BACKUP-DEBUG] cleanup: removing backup in progress from VMI (deleting: %v, done: %v)",
			isBackupDeleting(backup), IsBackupDone(backup.Status))
		if syncInfo := ctrl.removeSourceBackupInProgress(vmi); syncInfo != nil {
			log.Log.Infof("[BACKUP-DEBUG] cleanup: removeSourceBackupInProgress returned syncInfo: %+v", syncInfo)
			return false, syncInfo
		}
	}

	log.Log.Infof("[BACKUP-DEBUG] backup %s cleanup in final stages", backup.Name)
	if isBackupDeleting(backup) {
		log.Log.Infof("[BACKUP-DEBUG] removing backup %s finalizer", backup.Name)
		if syncInfo := ctrl.removeBackupFinalizer(backup); syncInfo != nil {
			log.Log.Infof("[BACKUP-DEBUG] cleanup: removeBackupFinalizer returned syncInfo: %+v", syncInfo)
			return false, syncInfo
		}
	}

	log.Log.Infof("[BACKUP-DEBUG] cleanup: cleanup completed for backup %s/%s", backup.Namespace, backup.Name)
	return true, nil
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
