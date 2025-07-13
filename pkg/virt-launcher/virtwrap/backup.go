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
	"os"
	"path/filepath"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"libvirt.org/go/libvirt"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/util"
	"kubevirt.io/kubevirt/pkg/virt-launcher/metadata"
	api "kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/api"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/cli"
	"kubevirt.io/kubevirt/pkg/virt-launcher/virtwrap/converter"
)

const (
	ChangedBlockTrackingNotEnabled = "Backup failed ChangedBlockTracking is not enabled"
	backupTimeXMLFormat            = "2006-01-02_15-04-05"
	freezeFailedMsg                = "Failed freezing guest filesystem: %s"
	unfreezeFailedMsg              = "Failed to unfreeze filesystem after backup completion"
)

func backupTimeFormatted(time *metav1.Time) string {
	return time.UTC().Format(backupTimeXMLFormat)
}

func (l *LibvirtDomainManager) initializeBackupMetadata(backupOptions *backupv1.BackupOptions, vmiName string) (bool, error) {
	backupMetadata, exists := l.metadataCache.Backup.Load()
	// Same start time is a unique indication for the backup
	// since backupname can be reused
	if exists && backupMetadata.StartTimestamp == backupOptions.BackupStartTime {
		if backupMetadata.EndTimestamp == nil {
			// backup is in progress already, ignore
			return true, nil
		} else {
			// backup already comppleted should not initialize the same backup again
			return false, fmt.Errorf("backup %s that started at %s already executed, finished at %v, completed: %t, failed: %t, abortStatus: %s",
				backupOptions.BackupName, *backupMetadata.StartTimestamp, *backupMetadata.EndTimestamp, backupMetadata.Completed, backupMetadata.Failed, backupMetadata.AbortStatus)
		}
	} else if exists {
		if backupMetadata.EndTimestamp == nil {
			// another backup already exists and have not completed yet
			return false, fmt.Errorf("backup %s already in progress, need to wait for completion", backupMetadata.Name)
		}
	}

	b := api.BackupMetadata{
		Name:           backupOptions.BackupName,
		StartTimestamp: backupOptions.BackupStartTime,
		CheckpointName: checkpointName(backupOptions.BackupName, backupTimeFormatted(backupOptions.BackupStartTime)),
		SkipQuiesce:    backupOptions.SkipQuiesce,
	}
	if backupOptions.PushPath != nil {
		b.BackupPath = getBackupPath(backupOptions, vmiName)
	}
	l.metadataCache.Backup.Store(b)
	log.Log.V(3).Infof("initialize backup metadata: %v", b)

	return false, nil
}

func (l *LibvirtDomainManager) backupVirtualMachine(vmi *v1.VirtualMachineInstance, backupOptions *backupv1.BackupOptions) error {
	log.Log.Object(vmi).Infof("backup begin called")
	if l.migrationInProgress() {
		return fmt.Errorf("Failed to do backup, VMI is currently during migration")
	}
	inProgress, err := l.initializeBackupMetadata(backupOptions, vmi.Name)
	if err != nil {
		log.Log.Object(vmi).Warning(err.Error())
		return err
	}
	if inProgress {
		log.Log.Info("backup already in progress")
		return nil
	}

	log.Log.Object(vmi).Infof("Initializing backup")
	err = l.backup(vmi, backupOptions)
	if err != nil {
		log.Log.Object(vmi).Reason(err).Error("Backup failed")
		return err
	}

	log.Log.Object(vmi).Infof("backup started")
	return nil
}

func (l *LibvirtDomainManager) backup(vmi *v1.VirtualMachineInstance, backupOptions *backupv1.BackupOptions) (failed error) {
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

	var backupPath string
	if backupOptions.PushPath != nil {
		backupPath = getBackupPath(backupOptions, vmi.Name)
		if err := util.MkdirAllWithNosec(backupPath); err != nil {
			return fmt.Errorf("error creating dir for backup: %v", err)
		}
		defer func(path string) {
			if failed != nil {
				logger.Reason(failed).Error("failed to do backup")
				if err := os.RemoveAll(path); err != nil {
					logger.Reason(err).Error("failed to clean up backup directory")
				}
			}
		}(backupPath)
	}
	domainBackup, domainCheckpoint := generateDomainBackup(domainDisks, backupOptions, backupPath)
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
	if !backupOptions.SkipQuiesce {
		logger.Info("Freezing VMI")
		if err := dom.FSFreeze(nil, 0); err != nil {
			logger.Warningf(freezeFailedMsg, err)
			l.metadataCache.Backup.WithSafeBlock(func(backupMetadata *api.BackupMetadata, _ bool) {
				backupMetadata.BackupMsg = fmt.Sprintf(freezeFailedMsg, err)
			})
		}
	}

	return dom.BackupBegin(strings.ToLower(string(backupXML)), strings.ToLower(string(checkpointXML)), 0)
}

func generateDomainBackup(disks []api.Disk, backupOptions *backupv1.BackupOptions, backupPath string) (*api.DomainBackup, *api.DomainCheckpoint) {
	log.Log.Infof("backup generateDomainBackup")
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
			backupDisk.Backup = "yes"
			backupDisk.Type = "file"
			if backupOptions.PushPath != nil {
				backupDisk.Target = &api.BackupTarget{
					File: targetQCOW2File(backupPath, backupOptions.BackupName, volumeName),
				}
			}
			checkpointDisk.Checkpoint = "bitmap"
		} else {
			backupDisk.Backup = "no"
			checkpointDisk.Checkpoint = "no"
		}
		backupDisks.Disks = append(backupDisks.Disks, backupDisk)
		checkpointDisks.Disks = append(checkpointDisks.Disks, checkpointDisk)
	}

	domainBackup.BackupDisks = backupDisks
	backupTime := backupTimeFormatted(backupOptions.BackupStartTime)
	domainCheckpoint := &api.DomainCheckpoint{
		Name:            checkpointName(backupOptions.BackupName, backupTime),
		CheckpointDisks: checkpointDisks,
	}
	return domainBackup, domainCheckpoint
}

func getBackupPath(backupOptions *backupv1.BackupOptions, vmiName string) string {
	backupTime := backupTimeFormatted(backupOptions.BackupStartTime)
	backupNameWithTime := fmt.Sprintf("%s-%s", backupOptions.BackupName, backupTime)
	return filepath.Join(*backupOptions.PushPath, vmiName, backupNameWithTime)
}

func targetQCOW2File(pushPath, backupName, volumeName string) string {
	fileName := fmt.Sprintf("%s-%s.qcow2", backupName, volumeName)
	return filepath.Join(pushPath, fileName)
}

func checkpointName(backupName, backupTime string) string {
	return fmt.Sprintf("%s-%s", backupName, backupTime)
}

func IsChangedBlockTrackingEnabled(vmi *v1.VirtualMachineInstance) bool {
	return vmi.Status.ChangedBlockTracking.State == v1.ChangedBlockTrackingEnabled
}

func IsChangedBlockTrackingInitializing(vmi *v1.VirtualMachineInstance) bool {
	return vmi.Status.ChangedBlockTracking.State == v1.ChangedBlockTrackingInitializing
}

// logBackupInfo logs the same backup info as `virsh -r domjobinfo`
func logBackupInfo(logger *log.FilteredLogger, backupName string, info *libvirt.DomainJobInfo) {
	logger.V(2).Infof("Backup %s info %+v", backupName, info)
}

func HandleBackupJobCompletedEvent(domain cli.VirDomain, event *libvirt.DomainEventJobCompleted, metadataCache *metadata.Cache) {
	backupMetadata, exists := metadataCache.Backup.Load()
	if !exists {
		log.Log.Warning("Received backup job completed event, but no active backup metadata found in cache. Ignoring event.")
		return
	}
	backupName := backupMetadata.Name
	logger := log.Log.With("backupName", backupName)

	backupMsg := ""
	if domain != nil {
		finalStats, err := domain.GetJobStats(libvirt.DOMAIN_JOB_STATS_COMPLETED)
		if err != nil {
			logger.Reason(err).Error("Failed to get final job stats for completed backup.")
		} else if finalStats != nil {
			event.Info.Type = finalStats.Type
			if finalStats.ErrorMessageSet {
				event.Info.ErrorMessageSet = true
				event.Info.ErrorMessage = finalStats.ErrorMessage
			}
			logBackupInfo(logger, backupName, finalStats)
		}
		if !backupMetadata.SkipQuiesce && backupMetadata.BackupMsg != freezeFailedMsg {
			if err := domain.FSThaw(nil, 0); err != nil {
				backupMsg = unfreezeFailedMsg
				logger.Reason(err).Error(backupMsg)
			}
		}
	}

	isFailed := false
	abortStatus := backupv1.BackupAbortUndefined

	switch event.Info.Type {
	case libvirt.DOMAIN_JOB_COMPLETED:
		logger.Info("Backup has been completed successfully")
	case libvirt.DOMAIN_JOB_FAILED:
		isFailed = true
		backupPath := backupMetadata.BackupPath
		if backupPath != "" {
			logger.Infof("Cleaning up failed backup directory: %s", backupPath)
			if err := os.RemoveAll(backupPath); err != nil {
				logger.Reason(err).Error("failed to clean up backup directory")
			}
		}
		backupMsg = fmt.Sprintf("Libvirt job result: %d", event.Info.Type)
		if event.Info.ErrorMessageSet {
			backupMsg = fmt.Sprintf("%s, error message: %s", backupMsg, event.Info.ErrorMessage)
		}
		logger.Error(backupMsg)
	case libvirt.DOMAIN_JOB_CANCELLED:
		isFailed = true
		abortStatus = backupv1.BackupAbortSucceeded
		backupMsg = fmt.Sprintf("Backup job was cancelled. Libvirt job result: %d", event.Info.Type)
		if event.Info.ErrorMessageSet {
			backupMsg = fmt.Sprintf("%s, error message: %s", backupMsg, event.Info.ErrorMessage)
		}
		logger.Warning(backupMsg)
	default:
		isFailed = true
		backupMsg = fmt.Sprintf("Backup job ended with unknown result: %d", event.Info.Type)
		logger.Warning(backupMsg)
	}

	metadataCache.Backup.WithSafeBlock(func(backupMetadata *api.BackupMetadata, exists bool) {
		if exists {
			backupMetadata.Completed = true
			backupMetadata.Failed = isFailed
			if backupMsg != "" {
				backupMetadata.BackupMsg = backupMsg
			}
			if abortStatus != backupv1.BackupAbortUndefined {
				backupMetadata.AbortStatus = string(abortStatus)
			}
			now := metav1.Now()
			backupMetadata.EndTimestamp = &now
		} else {
			log.Log.Warning("Attempted to update metadata cache, but no backup metadata loaded after event.")
		}
	})

	log.Log.V(2).Infof("Updated backup result in metadata via Notifier: %s", metadataCache.Backup.String())
}

func (l *LibvirtDomainManager) backupVirtualMachineAbort(vmi *v1.VirtualMachineInstance, backupOptions *backupv1.BackupOptions) error {
	log.Log.Infof("backup abort called")
	backupMetadata, exists := l.metadataCache.Backup.Load()
	if exists {
		if backupMetadata.StartTimestamp == backupOptions.BackupStartTime &&
			(backupMetadata.Completed || backupMetadata.EndTimestamp != nil) {
			log.Log.Infof("backup %s already completed, abort cancelled", backupOptions.BackupName)
			return nil
		}
		if backupMetadata.StartTimestamp != backupOptions.BackupStartTime {
			log.Log.Infof("abort was called for backup %s that started at: %v, but the latest backup is %s that started at %v. can't abort", backupOptions.BackupName, backupOptions.BackupStartTime, backupMetadata.Name, backupMetadata.StartTimestamp)
			return nil
		}
		if backupMetadata.AbortStatus == string(backupv1.BackupAbortInProgress) {
			log.Log.V(3).Infof("abort for backup %s is already in progress", backupOptions.BackupName)
			return nil
		}
	}

	domName := api.VMINamespaceKeyFunc(vmi)
	dom, err := l.virConn.LookupDomainByName(domName)
	if dom == nil || err != nil {
		return err
	}
	defer dom.Free()
	jobInfo, err := dom.GetJobInfo()
	if err != nil {
		log.Log.Reason(err).Error("failed to get domain job info")
		return err
	}
	if jobInfo.Type != libvirt.DOMAIN_JOB_BOUNDED {
		log.Log.Infof("job type is %v, no need to abort it", jobInfo.Type)
		return nil
	}
	l.metadataCache.Backup.WithSafeBlock(func(backupMetadata *api.BackupMetadata, exists bool) {
		if exists && !backupMetadata.Completed {
			backupMetadata.Failed = true
			backupMetadata.AbortStatus = string(backupv1.BackupAbortInProgress)
		}
	})

	err = dom.AbortJob()
	if err != nil {
		log.Log.Reason(err).Error("failed to abort backup")
		return err
	}
	return nil
}
