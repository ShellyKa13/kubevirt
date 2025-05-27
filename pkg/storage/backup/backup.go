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

package backup

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	hotplugdisk "kubevirt.io/kubevirt/pkg/hotplug-disk"
	"kubevirt.io/kubevirt/pkg/pointer"
	"kubevirt.io/kubevirt/pkg/storage/status"
	"kubevirt.io/kubevirt/pkg/storage/types"
	watchutil "kubevirt.io/kubevirt/pkg/virt-controller/watch/util"
)

const (
	backupInitiatedEvent = "VirtualMachineBackupInitiated"
	backupAbortedEvent   = "VirtualMachineBackupAborted"
	backupFailedEvent    = "VirtualMachineBackupFailed"
	backupCompletedEvent = "VirtualMachineBackupCompletedSuccessfully"

	backupTargetPVCDoesntExist        = "BackupTargetPVCDoesntExist"
	backupSourceDoesntExist           = "BackupSourceDoesntExist"
	backupSourceNotRunning            = "BackupSourceNotRunning"
	backupTargetPVCAddVolumeSubmitted = "backupTargetPVCAddVolumeSubmitted"

	backupInitializing = "backup is initializing"
	backupInProgress   = "Backup is in progress"
)

var (
	otherBackupInProgress = "another backup %s is already in progress"
	backupCompletedMsg    = "Successfully completed VirtualMachineBackup %s"
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

	if equality.Semantic.DeepEqual(ovmi.Status.VolumeStatus, nvmi.Status.VolumeStatus) &&
		equality.Semantic.DeepEqual(ovmi.Status.BackupStatus, nvmi.Status.BackupStatus) {
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
		ctrl.backupInformer.HasSynced,
	) {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	for i := 0; i < threadiness; i++ {
		go wait.Until(ctrl.backupWorker, time.Second, stopCh)
	}

	<-stopCh

	return nil
}

func (ctrl *VMBackupController) backupWorker() {
	for ctrl.processBackupWorkItem() {
	}
}

func (ctrl *VMBackupController) processBackupWorkItem() bool {
	return watchutil.ProcessWorkItem(ctrl.backupQueue, func(key string) (time.Duration, error) {
		log.Log.V(3).Infof("backup worker processing key [%s]", key)

		storeObj, exists, err := ctrl.backupInformer.GetStore().GetByKey(key)
		if !exists || err != nil {
			return 0, err
		}

		backup, ok := storeObj.(*backupv1.VirtualMachineBackup)
		if !ok {
			return 0, fmt.Errorf("unexpected resource %+v", storeObj)
		}

		return ctrl.exec(backup)
	})
}

type SyncInfo struct {
	err    error
	reason string
	event  string
}

func syncInfoError(err error) *SyncInfo {
	return &SyncInfo{err: err}
}

func (ctrl *VMBackupController) exec(backup *backupv1.VirtualMachineBackup) (time.Duration, error) {
	logger := log.Log.Object(backup)
	logger.V(4).Infof("Processing VirtualMachineBackup")

	syncInfo := ctrl.sync(backup)
	if syncInfo != nil && syncInfo.err != nil {
		return 0, syncInfo.err
	}

	err := ctrl.updateStatus(backup, syncInfo, logger)
	if err != nil {
		logger.Reason(err).Error("Updating the VirtualMachine status failed.")
		return 0, err
	}

	return 0, nil
}

func (ctrl *VMBackupController) sync(backup *backupv1.VirtualMachineBackup) *SyncInfo {
	vmi, err := ctrl.verifyBackupSource(backup)
	if err != nil {
		return err
	}
	if !isBackupInitializing(backup.Status) {
		return ctrl.checkBackupCompletion(backup, vmi)
	}
	if err := ctrl.updateSourceBackupInProgress(vmi, backup.Name); err != nil {
		return err
	}
	backupOptions := backupv1.BackupOptions{
		BackupName:      backup.Name,
		BackupStartTime: pointer.P(metav1.Now()),
		SkipQuiesce:     backup.Spec.SkipQuiesce,
	}
	switch *backup.Spec.Mode {
	case backupv1.PushMode:
		pvcName := backup.Spec.PvcName
		err = ctrl.verifyBackupTargetPVC(pvcName, backup.Namespace)
		if err != nil {
			return err
		}

		attached, syncInfo := ctrl.backupTargetPVCAttached(vmi, *pvcName)
		if syncInfo != nil {
			return syncInfo
		}
		if !attached {
			return ctrl.attachBackupTargetPVC(vmi, *pvcName)
		}
		backupOptions.Mode = backupv1.PushMode
		backupOptions.PushPath = pointer.P(hotplugdisk.GetVolumeMountDir(*pvcName))
	default:
		return syncInfoError(fmt.Errorf("Invalid backup mode"))
	}

	backupErr := ctrl.client.VirtualMachineInstance(vmi.Namespace).Backup(context.Background(), vmi.Name, &backupOptions)
	if backupErr != nil {
		return syncInfoError(backupErr)
	}

	return &SyncInfo{
		event:  backupInitiatedEvent,
		reason: backupInProgress,
	}
}

func (ctrl *VMBackupController) updateStatus(backup *backupv1.VirtualMachineBackup, syncInfo *SyncInfo, logger *log.FilteredLogger) error {
	backupOut := backup.DeepCopy()

	if backup.Status == nil {
		backupOut.Status = &backupv1.VirtualMachineBackupStatus{}
		updateBackupCondition(backupOut, newInitializingCondition(corev1.ConditionTrue, backupInitializing))
		updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, backupInitializing))
	}

	if syncInfo != nil {
		switch syncInfo.event {
		case backupInitiatedEvent:
			removeBackupCondition(backupOut, backupv1.ConditionInitializing)
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionTrue, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionFalse, syncInfo.reason))
		case backupAbortedEvent, backupFailedEvent:
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
			updateBackupCondition(backupOut, newFailureCondition(corev1.ConditionTrue, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionTrue, syncInfo.reason))
		case backupCompletedEvent:
			updateBackupCondition(backupOut, newProgressingCondition(corev1.ConditionFalse, syncInfo.reason))
			updateBackupCondition(backupOut, newDoneCondition(corev1.ConditionTrue, backupCompletedMsg))
			ctrl.recorder.Eventf(backupOut, corev1.EventTypeNormal, backupCompletedEvent, backupCompletedMsg, backupOut.Name)
			backupOut.Status.CheckpointName = syncInfo.reason
		}
	}

	if !equality.Semantic.DeepEqual(backup, backupOut) {
		return ctrl.backupStatusUpdater.UpdateStatus(backupOut)
	}
	return nil
}

func getSourceName(backup *backupv1.VirtualMachineBackup) string {
	if backup.Spec.Source != nil {
		return backup.Spec.Source.Name
	}
	return backup.Spec.BackupTracker.Spec.Source.Name
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
	return obj.(*v1.VirtualMachineInstance), nil
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

func (ctrl *VMBackupController) backupTargetPVCAttached(vmi *v1.VirtualMachineInstance, pvcName string) (bool, *SyncInfo) {
	for _, volumeStatus := range vmi.Status.VolumeStatus {
		if volumeStatus.Name == pvcName {
			if volumeStatus.HotplugVolume != nil &&
				volumeStatus.Phase == v1.HotplugVolumeMounted {
				return true, nil
			} else {
				return false, nil
			}
		}
	}
	return false, nil
}

func (ctrl *VMBackupController) attachBackupTargetPVC(vmi *v1.VirtualMachineInstance, pvcName string) *SyncInfo {
	volumeSource := &v1.HotplugVolumeSource{
		PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
			PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
			Hotpluggable: true,
		},
	}

	hotplugRequest := &v1.AddVolumeOptions{
		Name:         pvcName,
		VolumeSource: volumeSource,
	}
	err := ctrl.client.VirtualMachine(vmi.Namespace).AddVolume(context.Background(), vmi.Name, hotplugRequest)
	if err != nil {
		return syncInfoError(err)
	}
	// err = virtClient.VirtualMachineInstance(namespace).AddVolume(context.Background(), vmiName, hotplugRequest)

	// TODO: handle attach wait msg
	return &SyncInfo{
		event:  backupTargetPVCAddVolumeSubmitted,
		reason: fmt.Sprintf("backup target pvc %s add volume request submitted", vmi.Name),
	}
}

func (ctrl *VMBackupController) removeSourceBackupInProgress(vmi *v1.VirtualMachineInstance) *SyncInfo {
	vmiCopy := vmi.DeepCopy()
	vmiCopy.Status.BackupStatus = nil
	var err error
	vmiCopy, err = ctrl.client.VirtualMachineInstance(vmiCopy.Namespace).UpdateStatus(context.Background(), vmiCopy, metav1.UpdateOptions{})
	if err != nil {
		return syncInfoError(err)
	}

	return nil
}

func (ctrl *VMBackupController) updateSourceBackupInProgress(vmi *v1.VirtualMachineInstance, backupName string) *SyncInfo {
	if vmi.Status.BackupStatus != nil {
		if vmi.Status.BackupStatus.BackupName != backupName {
			return syncInfoError(fmt.Errorf(otherBackupInProgress, vmi.Status.BackupStatus.BackupName))
		}
		return nil
	}

	vmiCopy := vmi.DeepCopy()
	vmiCopy.Status.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
		BackupName: backupName,
	}
	var err error
	_, err = ctrl.client.VirtualMachineInstance(vmiCopy.Namespace).UpdateStatus(context.Background(), vmiCopy, metav1.UpdateOptions{})
	if err != nil {
		return syncInfoError(err)
	}

	return nil
}

func (ctrl *VMBackupController) checkBackupCompletion(backup *backupv1.VirtualMachineBackup, vmi *v1.VirtualMachineInstance) *SyncInfo {
	backupStatus := vmi.Status.BackupStatus
	if backupStatus == nil {
		return nil
	}

	if backupStatus.BackupName != backup.Name {
		return syncInfoError(fmt.Errorf(otherBackupInProgress, backupStatus.BackupName))
	}

	if !backupStatus.Completed {
		return nil
	}

	if isBackupDone(backup.Status) {
		ctrl.removeSourceBackupInProgress(vmi)
	}

	if backupStatus.AbortStatus != nil {
		return &SyncInfo{
			event:  backupAbortedEvent,
			reason: *backupStatus.AbortStatus,
		}
	}
	if backupStatus.Failed {
		return &SyncInfo{
			event:  backupFailedEvent,
			reason: *backupStatus.FailureReason,
		}
	}

	return &SyncInfo{
		event:  backupCompletedEvent,
		reason: backupStatus.CheckpointName,
	}
}

func isBackupInitializing(status *backupv1.VirtualMachineBackupStatus) bool {
	return status == nil || hasCondition(status.Conditions, backupv1.ConditionInitializing)
}

func isBackupDone(status *backupv1.VirtualMachineBackupStatus) bool {
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
