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
 * Copyright 2025 Red Hat, Inc.
 *
 */

package virtwrap

import (
	"encoding/xml"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"libvirt.org/go/libvirt"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"

	api "kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/cli"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/converter"
	domainerrors "kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/errors"
	stats "kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/stats"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/statsconv"
)

const (
	ChangedBlockTrackingNotEnabled    = "Backup failed Changed Block Tracking is not enabled"
	backupTimeFormat                  = "20060102T150405Z07"
	defaultBackupProgressStuckTimeout = 150
	defaultAcceptableCompletionTime   = 150
)

func (l *LibvirtDomainManager) initializeBackupMetadata(backupOptions *backupv1.BackupOptions) (bool, error) {
	backupMetadata, exists := l.metadataCache.Backup.Load()
	if exists && backupMetadata.StartTimestamp == backupOptions.BackupStartTime {
		if backupMetadata.EndTimestamp == nil {
			// don't stop on currently executing backups
			return true, nil
		} else {
			return false, fmt.Errorf("backup %s that started at %s already executed, finished at %v, completed: %t, failed: %t, abortStatus: %s",
				backupOptions.BackupName, *backupMetadata.StartTimestamp, *backupMetadata.EndTimestamp, backupMetadata.Completed, backupMetadata.Failed, backupMetadata.AbortStatus)
		}
	}

	b := api.BackupMetadata{
		Name:           backupOptions.BackupName,
		StartTimestamp: backupOptions.BackupStartTime,
		CheckpointName: checkpointName(backupOptions.BackupName, backupOptions.BackupStartTime.Format(backupTimeFormat)),
	}
	l.metadataCache.Backup.Store(b)
	log.Log.V(3).Infof("initialize backup metadata: %v", b)

	return false, nil
}

func (l *LibvirtDomainManager) backupBegin(vmi *v1.VirtualMachineInstance, backupOptions *backupv1.BackupOptions) {
	inProgress, err := l.initializeBackupMetadata(backupOptions)
	if err != nil {
		log.Log.Object(vmi).Warning(err.Error())
		return
	}
	if inProgress {
		log.Log.V(3).Info("backup already in progress")
		return
	}

	go l.backup(vmi, backupOptions)
}

func (l *LibvirtDomainManager) backup(vmi *v1.VirtualMachineInstance, backupOptions *backupv1.BackupOptions) {
	log.Log.Object(vmi).Infof("Initiating backup")

	backupErrorChan := make(chan error, 1)

	// From here on out, any error encountered must be sent to the
	// backupErrorChan channel which is processed by the backupMonitor
	// go routine.
	monitor := newBackupMonitor(vmi, backupOptions.BackupName, l, backupErrorChan)
	go monitor.startMonitor()

	err := l.backupHelper(vmi, backupOptions)
	if err != nil {
		log.Log.Object(vmi).Reason(err).Error("Backup failed")
		backupErrorChan <- err
		return
	}

	log.Log.Object(vmi).Infof("Backup started")
}

func (l *LibvirtDomainManager) backupHelper(vmi *v1.VirtualMachineInstance, backupOptions *backupv1.BackupOptions) error {
	logger := log.Log.Object(vmi)
	domName := api.VMINamespaceKeyFunc(vmi)
	dom, err := l.virConn.LookupDomainByName(domName)
	if dom == nil || err != nil {
		return err
	}
	defer dom.Free()
	domainDisks, err := getAllDomainDisks(dom)
	if err != nil {
		log.Log.Object(vmi).Reason(err).Error("failed to parse domain XML to get disks.")
		return err
	}

	domainBackup, domainCheckpoint := generateDomainBackup(domainDisks, backupOptions)
	backupXML, err := xml.Marshal(domainBackup)
	if err != nil {
		logger.Reason(err).Error("marshalling backup xml failed")
		return err
	}
	checkpointXML, err := xml.Marshal(domainCheckpoint)
	if err != nil {
		logger.Reason(err).Error("marshalling checkpoint xml failed")
		return err
	}
	err = dom.BackupBegin(strings.ToLower(string(backupXML)), strings.ToLower(string(checkpointXML)), 0)
	return err
}

func generateDomainBackup(disks []api.Disk, backupOptions *backupv1.BackupOptions) (*api.DomainBackup, *api.DomainCheckpoint) {
	backupTime := backupOptions.BackupStartTime.Format(backupTimeFormat)
	domainBackup := &api.DomainBackup{
		Mode: string(backupOptions.Mode),
	}
	if backupOptions.Incremental != nil && *backupOptions.Incremental != "" {
		domainBackup.Incremental = backupOptions.Incremental
	}
	backupDisks := &api.BackupDisks{}
	checkpointDisks := &api.CheckpointDisks{}
	// the name of the volume should match the alias
	for _, disk := range disks {
		if disk.Target.Device == "" {
			continue
		}
		backupDisk := api.BackupDisk{
			Name: disk.Target.Device,
		}
		checkpointDisk := api.CheckpointDisk{
			Name: disk.Target.Device,
		}
		volumeName := converter.GetVolumeNameByDisk(disk)
		if disk.Source.DataStore != nil {
			log.Log.Infof("backup volume %s", volumeName)
			backupDisk.Backup = "yes"
			backupDisk.Type = "file"
			if backupOptions.PushPath != nil {
				backupDisk.Target = &api.BackupTarget{
					File: targetPath(*backupOptions.PushPath, backupOptions.BackupName, volumeName, backupTime),
				}
			}
			checkpointDisk.Checkpoint = "bitmap"
		} else {
			log.Log.Infof("skip backup volume %s\n", volumeName)
			backupDisk.Backup = "no"
			checkpointDisk.Checkpoint = "no"
		}
		backupDisks.Disks = append(backupDisks.Disks, backupDisk)
		checkpointDisks.Disks = append(checkpointDisks.Disks, checkpointDisk)
	}

	domainBackup.BackupDisks = backupDisks
	domainCheckpoint := &api.DomainCheckpoint{
		Name:            checkpointName(backupOptions.BackupName, backupTime),
		CheckpointDisks: checkpointDisks,
	}
	return domainBackup, domainCheckpoint
}

func targetPath(pushPath, backupName, volumeName, backupTime string) string {
	fileName := fmt.Sprintf("%s-%s-%s.qcow2", backupName, volumeName, backupTime)
	return filepath.Join(pushPath, fileName)
}

func checkpointName(backupName, backupTime string) string {
	return fmt.Sprintf("%s-%s", backupName, backupTime)
}

type backupMonitor struct {
	l          *LibvirtDomainManager
	vmi        *v1.VirtualMachineInstance
	backupName string

	backupErrorChan chan error

	start              int64
	lastProgressUpdate int64
	progressWatermark  uint64
	remainingData      uint64

	progressTimeout          int64
	acceptableCompletionTime int64
	backupFailedWithError    error
}

type inflightBackupAborted struct {
	message     string
	abortStatus backupv1.BackupAbortStatus
}

func newBackupMonitor(vmi *v1.VirtualMachineInstance, backupName string, l *LibvirtDomainManager, backupErrorChan chan error) *backupMonitor {
	monitor := &backupMonitor{
		l:                        l,
		vmi:                      vmi,
		backupName:               backupName,
		backupErrorChan:          backupErrorChan,
		progressWatermark:        0,
		remainingData:            0,
		progressTimeout:          defaultBackupProgressStuckTimeout,
		acceptableCompletionTime: defaultAcceptableCompletionTime,
	}

	return monitor
}

func (b *backupMonitor) startMonitor() {
	var completedJobInfo *libvirt.DomainJobInfo
	vmi := b.vmi

	b.start = time.Now().UTC().UnixNano()
	b.lastProgressUpdate = b.start

	logger := log.Log.Object(vmi)
	defer func() {
		close(b.backupErrorChan)
		b.l.domainInfoStats = &stats.DomainJobInfo{}
	}()

	domName := api.VMINamespaceKeyFunc(vmi)
	dom, err := b.l.virConn.LookupDomainByName(domName)
	if err != nil {
		logger.Reason(err).Error("Backup failed")
		return
	}
	defer dom.Free()
	logInterval := 0

	for {
		err = nil
		select {
		case err = <-b.backupErrorChan:
		case <-time.After(monitorSleepPeriodMS * time.Millisecond):
		}

		if err != nil && b.backupFailedWithError == nil {
			logger.Reason(err).Error("received a backup error")
			b.backupFailedWithError = err
		} else if b.backupFailedWithError != nil {
			logger.Info("Didn't manage to get a job status. Post the received error and finalize.")
			logger.Reason(b.backupFailedWithError).Error("backup failed")
			var abortStatus backupv1.BackupAbortStatus
			if strings.Contains(b.backupFailedWithError.Error(), "canceled by client") {
				abortStatus = backupv1.BackupAbortSucceeded
			}
			b.l.setBackupResult(true, true, fmt.Sprintf("Backup failed %v", b.backupFailedWithError.Error()), abortStatus)
			return
		}

		stats := completedJobInfo
		if stats == nil {
			stats, err = dom.GetJobStats(0)
			if err != nil {
				logger.Reason(err).Warning("failed to get domain job info, will retry")
				continue
			}
			if stats == nil {
				logger.Warning("stats is still nill, will retry")
				continue
			}
		}

		if stats.DataRemainingSet {
			b.remainingData = stats.DataRemaining
		}

		// b.processBackup(dom, stats)
		switch stats.Type {
		case libvirt.DOMAIN_JOB_UNBOUNDED:
			aborted := b.processInflightBackup(dom, stats)
			if aborted != nil {
				logger.Errorf("Backup abort detected with reason: %s", aborted.message)
				b.l.setBackupResult(true, true, aborted.message, aborted.abortStatus)
				return
			}
			logInterval++
			if logInterval%monitorLogInterval == 0 {
				logBackupInfo(logger, b.backupName, stats)
			}
		case libvirt.DOMAIN_JOB_NONE:
			logger.Warning("Got stats type DOMAIN_JOB_NONE in backup job")
			// if stats != nil {
			// 	fmt.Println("backup logging DOMAIN_JOB_NONE")
			// 	logBackupInfo(logger, b.backupName, stats)
			// }
			completedJobInfo = b.determineNonRunningBackupStatus(dom)
		case libvirt.DOMAIN_JOB_COMPLETED:
			// stats, err = dom.GetJobStats(libvirt.DOMAIN_JOB_STATS_COMPLETED)
			// if err != nil {
			// 	logger.Reason(err).Warning("Job completed but failed to get domain job info")
			// } else if stats == nil {
			// 	logger.Warning("Job completed but failed to get domain job info stats is nill")
			// } else {
			// 	logBackupInfo(logger, b.backupName, stats)
			// }
			logger.Info("Backup has been completed")
			b.l.setBackupResult(true, false, "", "")
			return
		case libvirt.DOMAIN_JOB_FAILED:
			logger.Warning("Backup job failed")
			b.l.setBackupResult(true, true, b.backupFailedWithError.Error(), "")
			return
		case libvirt.DOMAIN_JOB_CANCELLED:
			logger.Info("Backup was canceled")
			b.l.setBackupResult(true, true, "Backup aborted", backupv1.BackupAbortSucceeded)
			return
		}
	}
}

func (l *LibvirtDomainManager) setBackupResult(completed, failed bool, reason string, abortStatus backupv1.BackupAbortStatus) error {
	backupMetadata, exists := l.metadataCache.Backup.Load()
	if !exists {
		// nothing to report if backup metadata is empty
		return nil
	}

	if abortStatus != "" {
		metaAbortStatus := backupMetadata.AbortStatus
		if metaAbortStatus == string(abortStatus) && metaAbortStatus == string(backupv1.BackupAbortInProgress) {
			return domainerrors.BackupAbortInProgressError
		}
	}
	l.metadataCache.Backup.WithSafeBlock(func(backupMetadata *api.BackupMetadata, _ bool) {
		if abortStatus != "" {
			backupMetadata.AbortStatus = string(abortStatus)
		}

		if failed {
			backupMetadata.Failed = true
			backupMetadata.FailureReason = reason
		}
		if completed {
			backupMetadata.Completed = true
			now := metav1.Now()
			backupMetadata.EndTimestamp = &now
		}
	})

	logger := log.Log.V(2)
	if !failed {
		logger = logger.V(4)
	}
	logger.Infof("set backup result in metadata: %s", l.metadataCache.Backup.String())
	return nil
}

func (b *backupMonitor) processBackup(dom cli.VirDomain, stats *libvirt.DomainJobInfo) {
	now := time.Now().UTC().UnixNano()

	b.l.domainInfoStats = statsconv.Convert_libvirt_DomainJobInfo_To_stats_DomainJobInfo(stats)
	if (b.progressWatermark == 0) || (b.remainingData < b.progressWatermark) {
		b.lastProgressUpdate = now
	}
	b.progressWatermark = b.remainingData
}

// logBackupInfo logs the same backup info as `virsh -r domjobinfo`
func logBackupInfo(logger *log.FilteredLogger, backupName string, info *libvirt.DomainJobInfo) {
	bToMiB := func(bytes uint64) uint64 {
		return bytes / 1024 / 1024
	}

	bpsToMbps := func(bytes uint64) uint64 {
		return bytes * 8 / 1000000
	}

	logger.V(2).Info(fmt.Sprintf(`Backup info for %s: Type: %v, operation: %v, TimeElapsed:%dms DataProcessed:%dMiB DataRemaining:%dMiB DataTotal:%dMiB `+
		`MemoryProcessed:%dMiB MemoryRemaining:%dMiB MemoryTotal:%dMiB MemoryBandwidth:%dMbps DirtyRate:%dMbps `+
		`Iteration:%d PostcopyRequests:%d ConstantPages:%d NormalPages:%d NormalData:%dMiB ExpectedDowntime:%dms `+
		`DiskMbps:%d`,
		backupName, info.Type, info.Operation, info.TimeElapsed, bToMiB(info.DataProcessed), bToMiB(info.DataRemaining), bToMiB(info.DataTotal),
		bToMiB(info.MemProcessed), bToMiB(info.MemRemaining), bToMiB(info.MemTotal), bpsToMbps(info.MemBps), bpsToMbps(info.MemDirtyRate*info.MemPageSize),
		info.MemIteration, info.MemPostcopyReqs, info.MemConstant, info.MemNormal, bToMiB(info.MemNormalBytes), info.Downtime,
		bpsToMbps(info.DiskBps),
	))
}

func (b *backupMonitor) processInflightBackup(dom cli.VirDomain, stats *libvirt.DomainJobInfo) *inflightBackupAborted {
	logger := log.Log.Object(b.vmi)

	// Backup is running
	now := time.Now().UTC().UnixNano()
	elapsed := now - b.start

	b.l.domainInfoStats = statsconv.Convert_libvirt_DomainJobInfo_To_stats_DomainJobInfo(stats)
	if (b.progressWatermark == 0) || (b.remainingData < b.progressWatermark) {
		b.lastProgressUpdate = now
	}
	b.progressWatermark = b.remainingData

	switch {
	case !b.isBackupProgressing():
		err := dom.AbortJob()
		if err != nil {
			logger.Reason(err).Error("failed to abort backup")
			return nil
		}

		progressDelay := now - b.lastProgressUpdate
		aborted := &inflightBackupAborted{
			message:     fmt.Sprintf("B stuck for %d seconds and has been aborted", progressDelay/int64(time.Second)),
			abortStatus: backupv1.BackupAbortSucceeded,
		}
		return aborted
	case b.shouldTriggerTimeout(elapsed):
		// check the overall backup time
		// if the total backup time exceeds an acceptable
		// limit, then the backup will get aborted
		err := dom.AbortJob()
		if err != nil {
			logger.Reason(err).Error("failed to abort backup")
			return nil
		}

		aborted := &inflightBackupAborted{
			message:     fmt.Sprintf("B is not completed after %d seconds and has been aborted", b.acceptableCompletionTime),
			abortStatus: backupv1.BackupAbortSucceeded,
		}
		return aborted
	}

	return nil
}

func (b *backupMonitor) determineNonRunningBackupStatus(dom cli.VirDomain) *libvirt.DomainJobInfo {
	logger := log.Log.Object(b.vmi)
	if b.lastProgressUpdate > b.start {
		logger.Info("Backup job has probably completed before we could capture the status. Getting latest status.")
		// at this point the backup is over, but we don't know the result.
		// check if we were trying to cancel this job. In this case, finalize the backup.
		backupMetadata, _ := b.l.metadataCache.Backup.Load()
		if backupMetadata.AbortStatus == string(backupv1.BackupAbortInProgress) {
			logger.Info("Backup job was canceled")
			return &libvirt.DomainJobInfo{
				Type:             libvirt.DOMAIN_JOB_CANCELLED,
				DataRemaining:    b.remainingData,
				DataRemainingSet: true,
			}
		}
		// return &libvirt.DomainJobInfo{
		// 	Type:             libvirt.DOMAIN_JOB_COMPLETED,
		// 	DataRemaining:    b.remainingData,
		// 	DataRemainingSet: true,
		// }
	}
	logger.Info("backup job didn't start yet")
	return nil
}

func (b *backupMonitor) isBackupProgressing() bool {
	logger := log.Log.Object(b.vmi)

	now := time.Now().UTC().UnixNano()

	// check if the backup is progressing
	progressDelay := (now - b.lastProgressUpdate) / int64(time.Second)
	if b.progressTimeout != 0 && progressDelay > b.progressTimeout {
		logger.Warningf("Live backup stuck for %d seconds", progressDelay)
		return false
	}

	return true
}

func (b *backupMonitor) shouldTriggerTimeout(elapsed int64) bool {
	if b.acceptableCompletionTime == 0 {
		return false
	}

	return elapsed/int64(time.Second) > b.acceptableCompletionTime
}

func IsChangedBlockTrackingEnabled(vmi *v1.VirtualMachineInstance) bool {
	return vmi.Status.ChangedBlockTracking == v1.ChangedBlockTrackingEnabled
}

func IsChangedBlockTrackingInitializing(vmi *v1.VirtualMachineInstance) bool {
	return vmi.Status.ChangedBlockTracking == v1.ChangedBlockTrackingInitializing
}
