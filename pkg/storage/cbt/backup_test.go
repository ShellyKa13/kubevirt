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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"kubevirt.io/kubevirt/pkg/libvmi"
	"kubevirt.io/kubevirt/pkg/pointer"
	"kubevirt.io/kubevirt/pkg/storage/status"
)

var _ = Describe("VMBackup Controller", func() {
	var (
		ctrl             *gomock.Controller
		virtClient       *kubecli.MockKubevirtClient
		vmiInterface     *kubecli.MockVirtualMachineInstanceInterface
		vmInterface      *kubecli.MockVirtualMachineInterface
		k8sClient        *k8sfake.Clientset
		backupController *VMBackupController
		testVMI          *v1.VirtualMachineInstance
		testBackup       *backupv1.VirtualMachineBackup
		testPVC          *corev1.PersistentVolumeClaim
		recorder         *record.FakeRecorder
		backupInformer   cache.SharedIndexInformer
		vmInformer       cache.SharedIndexInformer
		vmiInformer      cache.SharedIndexInformer
		pvcInformer      cache.SharedIndexInformer
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		virtClient = kubecli.NewMockKubevirtClient(ctrl)
		vmiInterface = kubecli.NewMockVirtualMachineInstanceInterface(ctrl)
		vmInterface = kubecli.NewMockVirtualMachineInterface(ctrl)
		k8sClient = k8sfake.NewSimpleClientset()

		recorder = record.NewFakeRecorder(100)

		// Setup informers
		backupInformer = cache.NewSharedIndexInformer(nil, &backupv1.VirtualMachineBackup{}, 0, cache.Indexers{})
		vmInformer = cache.NewSharedIndexInformer(nil, &v1.VirtualMachine{}, 0, cache.Indexers{})
		vmiInformer = cache.NewSharedIndexInformer(nil, &v1.VirtualMachineInstance{}, 0, cache.Indexers{})
		pvcInformer = cache.NewSharedIndexInformer(nil, &corev1.PersistentVolumeClaim{}, 0, cache.Indexers{})

		virtClient.EXPECT().VirtualMachineInstance(gomock.Any()).Return(vmiInterface).AnyTimes()
		virtClient.EXPECT().VirtualMachine(gomock.Any()).Return(vmInterface).AnyTimes()
		virtClient.EXPECT().CoreV1().Return(k8sClient.CoreV1()).AnyTimes()

		backupController = &VMBackupController{
			client:              virtClient,
			backupInformer:      backupInformer,
			vmStore:             vmInformer.GetStore(),
			vmiStore:            vmiInformer.GetStore(),
			pvcStore:            pvcInformer.GetStore(),
			recorder:            recorder,
			backupStatusUpdater: status.NewBackupStatusUpdater(virtClient),
		}

		testVMI = libvmi.New(
			libvmi.WithNamespace("default"),
			libvmi.WithName("test-vmi"),
		)
		testVMI.Status.ChangedBlockTracking = &v1.ChangedBlockTrackingStatus{}

		testBackup = &backupv1.VirtualMachineBackup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-backup",
				Namespace: "default",
				UID:       "test-uid",
			},
			Spec: backupv1.VirtualMachineBackupSpec{
				Source: &corev1.TypedLocalObjectReference{
					Kind:     "VirtualMachineInstance",
					Name:     "test-vmi",
					APIGroup: pointer.P(v1.GroupVersion.Group),
				},
				Mode:    pointer.P(backupv1.PushMode),
				PvcName: pointer.P("backup-target-pvc"),
			},
		}

		testPVC = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "backup-target-pvc",
				Namespace: "default",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		}
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	Context("verifyBackupSource", func() {
		It("should fail when VM doesn't exist", func() {
			vmi, syncInfo := backupController.verifyBackupSource(testBackup)
			Expect(vmi).To(BeNil())
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupSourceDoesntExist))
		})

		It("should fail when VMI doesn't exist", func() {
			testVM := &v1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vmi",
					Namespace: "default",
				},
			}
			vmInformer.GetStore().Add(testVM)

			vmi, syncInfo := backupController.verifyBackupSource(testBackup)
			Expect(vmi).To(BeNil())
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupSourceNotRunning))
		})

		It("should fail when VMI has no volumes", func() {
			testVM := &v1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vmi",
					Namespace: "default",
				},
			}
			vmInformer.GetStore().Add(testVM)
			testVMI.Spec.Volumes = []v1.Volume{}
			vmiInformer.GetStore().Add(testVMI)

			vmi, syncInfo := backupController.verifyBackupSource(testBackup)
			Expect(vmi).To(BeNil())
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupSourceNoVolumesToBackup))
		})

		It("should succeed when VM and VMI exist and VMI has volumes", func() {
			testVM := &v1.VirtualMachine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-vmi",
					Namespace: "default",
				},
			}
			vmInformer.GetStore().Add(testVM)
			testVMI.Spec.Volumes = []v1.Volume{
				{
					Name: "disk1",
				},
			}
			vmiInformer.GetStore().Add(testVMI)

			vmi, syncInfo := backupController.verifyBackupSource(testBackup)
			Expect(vmi).NotTo(BeNil())
			Expect(syncInfo).To(BeNil())
		})
	})

	Context("verifyBackupTargetPVC", func() {
		It("should fail when PVC doesn't exist in store", func() {
			pvcName := "nonexistent"
			syncInfo := backupController.verifyBackupTargetPVC(&pvcName, "default")
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupTargetPVCDoesntExist))
		})

		It("should fail when PVC doesn't exist", func() {
			pvcName := "non-existent-pvc"
			syncInfo := backupController.verifyBackupTargetPVC(&pvcName, "default")
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupTargetPVCDoesntExist))
		})

		It("should succeed when PVC exists", func() {
			pvcInformer.GetStore().Add(testPVC)
			pvcName := "backup-target-pvc"
			syncInfo := backupController.verifyBackupTargetPVC(&pvcName, "default")
			Expect(syncInfo).To(BeNil())
		})
	})

	Context("getVMI", func() {
		It("should return nil when VMI doesn't exist", func() {
			vmi, exists, err := backupController.getVMI(testBackup)
			Expect(vmi).To(BeNil())
			Expect(exists).To(BeFalse())
			Expect(err).To(BeNil())
		})

		It("should return VMI when it exists", func() {
			vmiInformer.GetStore().Add(testVMI)

			vmi, exists, err := backupController.getVMI(testBackup)
			Expect(vmi).NotTo(BeNil())
			Expect(exists).To(BeTrue())
			Expect(err).To(BeNil())
			Expect(vmi.Name).To(Equal("test-vmi"))
		})
	})

	Context("updateSourceBackupInProgress", func() {
		It("should update VMI status with backup name", func() {
			vmiInterface.EXPECT().Patch(
				context.Background(),
				testVMI.Name,
				k8stypes.JSONPatchType,
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
				patchStr := string(data)
				Expect(patchStr).To(ContainSubstring("/status/backupStatus"))
				Expect(patchStr).To(ContainSubstring("test-backup"))
				return testVMI, nil
			})

			err := backupController.updateSourceBackupInProgress(testVMI, testBackup.Name)
			Expect(err).To(BeNil())
		})

		It("should return error when another backup is in progress", func() {
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName: "another-backup",
			}

			err := backupController.updateSourceBackupInProgress(testVMI, testBackup.Name)
			Expect(err).NotTo(BeNil())
			Expect(err.Error()).To(ContainSubstring("another-backup"))
		})

		It("should not patch when backup name matches", func() {
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName: testBackup.Name,
			}

			err := backupController.updateSourceBackupInProgress(testVMI, testBackup.Name)
			Expect(err).To(BeNil())
		})
	})

	Context("removeSourceBackupInProgress", func() {
		It("should return nil when VMI is nil", func() {
			syncInfo := backupController.removeSourceBackupInProgress(nil)
			Expect(syncInfo).To(BeNil())
		})

		It("should return nil when ChangedBlockTracking is nil", func() {
			testVMI.Status.ChangedBlockTracking = nil
			syncInfo := backupController.removeSourceBackupInProgress(testVMI)
			Expect(syncInfo).To(BeNil())
		})

		It("should return nil when BackupStatus is nil", func() {
			testVMI.Status.ChangedBlockTracking.BackupStatus = nil
			syncInfo := backupController.removeSourceBackupInProgress(testVMI)
			Expect(syncInfo).To(BeNil())
		})

		It("should remove backup status from VMI", func() {
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName: testBackup.Name,
			}

			vmiInterface.EXPECT().Patch(
				context.Background(),
				testVMI.Name,
				k8stypes.JSONPatchType,
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
				patchStr := string(data)
				Expect(patchStr).To(ContainSubstring("/status/backupStatus"))
				Expect(patchStr).To(ContainSubstring("\"op\":\"remove\""))
				return testVMI, nil
			})

			syncInfo := backupController.removeSourceBackupInProgress(testVMI)
			Expect(syncInfo).To(BeNil())
		})
	})

	Context("checkBackupCompletion", func() {
		It("should call cleanup when backup is done", func() {
			testBackup.Status = &backupv1.VirtualMachineBackupStatus{
				Conditions: []backupv1.Condition{
					{
						Type:   backupv1.ConditionDone,
						Status: corev1.ConditionTrue,
					},
				},
			}

			syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
			Expect(syncInfo).To(BeNil()) // cleanup returns nil when nothing to clean
		})

		It("should return nil when backup not completed", func() {
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName: testBackup.Name,
				Completed:  false,
			}

			syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
			Expect(syncInfo).To(BeNil())
		})

		It("should return aborted event when backup is aborted", func() {
			abortMsg := string(backupv1.BackupAbortSucceeded)
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName:  testBackup.Name,
				Completed:   true, // Abort completes the backup
				AbortStatus: &abortMsg,
			}

			// No VMI patch expected during pre-completion cleanup (phased cleanup)

			syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupAbortedEvent))
			Expect(syncInfo.reason).To(Equal(abortMsg))
		})

		It("should return failed event when backup failed", func() {
			failMsg := "Backup failed"
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName: testBackup.Name,
				Completed:  true,
				Failed:     true,
				BackupMsg:  &failMsg,
			}

			// No VMI patch expected during pre-completion cleanup (phased cleanup)

			syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupFailedEvent))
			Expect(syncInfo.reason).To(Equal(failMsg))
		})

		It("should return completed event when backup succeeded", func() {
			checkpointName := "test-checkpoint"
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName:     testBackup.Name,
				Completed:      true,
				Failed:         false,
				CheckpointName: checkpointName,
			}

			// No VMI patch expected during pre-completion cleanup (phased cleanup)

			syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupCompletedEvent))
			Expect(syncInfo.checkpointName).To(Equal(checkpointName))
		})

		It("should return completed with warning when backup has warning message", func() {
			warningMsg := "Some disks were skipped"
			checkpointName := "test-checkpoint"
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName:     testBackup.Name,
				Completed:      true,
				Failed:         false,
				BackupMsg:      &warningMsg,
				CheckpointName: checkpointName,
			}

			// No VMI patch expected during pre-completion cleanup (phased cleanup)

			syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupCompletedWithWarningEvent))
			Expect(syncInfo.reason).To(ContainSubstring(warningMsg))
		})
	})

	Context("abortBackup", func() {
		It("should return aborted event when VMI is nil", func() {
			syncInfo := backupController.abortBackup(testBackup, nil)
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupAbortedEvent))
		})

		It("should return nil when no backup status", func() {
			testVMI.Status.ChangedBlockTracking = nil
			syncInfo := backupController.abortBackup(testBackup, testVMI)
			Expect(syncInfo).To(BeNil())
		})

		It("should send abort command to VMI", func() {
			testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
				BackupName: testBackup.Name,
			}

			vmiInterface.EXPECT().Backup(
				context.Background(),
				testVMI.Name,
				gomock.Any(),
			).DoAndReturn(func(ctx context.Context, name string, opts *backupv1.BackupOptions) error {
				Expect(opts.Cmd).To(Equal(backupv1.Abort))
				return nil
			})

			syncInfo := backupController.abortBackup(testBackup, testVMI)
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupAbortedEvent))
		})
	})

	Context("cleanup with utility volumes", func() {
		BeforeEach(func() {
			// Reset testBackup to ensure clean state
			testBackup.DeletionTimestamp = nil
			testBackup.Finalizers = nil
			testBackup.Status = nil

			testVMI.Status.Phase = v1.Running
			testVMI.Status.VolumeStatus = nil // Reset volume status
			testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{
				{
					Name: backupTargetPVC,
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "backup-target-pvc",
					},
					Type: pointer.P(v1.Backup),
				},
			}
			// Add VolumeStatus to indicate the volume is still attached
			testVMI.Status.VolumeStatus = []v1.VolumeStatus{
				{
					Name:          "backup-target-pvc",
					HotplugVolume: &v1.HotplugVolumeStatus{},
					Phase:         v1.VolumeReady,
				},
			}
		})

		It("should detach backup target PVC during cleanup", func() {
			vmiInterface.EXPECT().Patch(
				context.Background(),
				testVMI.Name,
				k8stypes.JSONPatchType,
				gomock.Any(),
				gomock.Any(),
			).DoAndReturn(
				func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
					patchStr := string(data)
					Expect(patchStr).To(Or(
						ContainSubstring("\"op\":\"remove\""),
						ContainSubstring("\"op\":\"replace\""),
					))
					return testVMI, nil
				}).Times(1)

			done, syncInfo := backupController.cleanup(testBackup, testVMI)
			Expect(done).To(BeFalse())
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupTargetPVCRemoveVolumeSubmitted))
		})

		It("should return done when VMI is nil", func() {
			done, syncInfo := backupController.cleanup(testBackup, nil)
			Expect(done).To(BeTrue())
			Expect(syncInfo).To(BeNil())
		})

		It("should return done when no utility volumes and no backup status", func() {
			testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{}
			testVMI.Status.VolumeStatus = nil
			testVMI.Status.ChangedBlockTracking.BackupStatus = nil

			// No patch expected since backup status is already nil and no volumes attached
			done, syncInfo := backupController.cleanup(testBackup, testVMI)
			Expect(done).To(BeTrue())
			Expect(syncInfo).To(BeNil())
		})
	})

	Context("Integration: utility volumes lifecycle", func() {
		It("should attach and detach utility volume", func() {
			pvcInformer.GetStore().Add(testPVC)

			// Step 1: Attach utility volume
			vmiInterface.EXPECT().Patch(context.Background(), testVMI.Name, k8stypes.JSONPatchType, gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
					patchStr := string(data)
					Expect(patchStr).To(ContainSubstring("/spec/utilityVolumes"))
					Expect(patchStr).To(ContainSubstring(backupTargetPVC))
					return testVMI, nil
				})

			syncInfo := backupController.attachBackupTargetPVC(testVMI, "backup-target-pvc")
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupTargetPVCAddVolumeSubmitted))

			// Step 2: Detach utility volume
			testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{
				{
					Name: backupTargetPVC,
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "backup-target-pvc",
					},
					Type: pointer.P(v1.Backup),
				},
			}

			vmiInterface.EXPECT().Patch(context.Background(), testVMI.Name, k8stypes.JSONPatchType, gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
					patchStr := string(data)
					Expect(patchStr).To(ContainSubstring("\"op\":\"remove\""))
					Expect(patchStr).To(ContainSubstring("/spec/utilityVolumes"))
					return testVMI, nil
				})

			syncInfo = backupController.detachBackupTargetPVC(testVMI, "backup-target-pvc")
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.event).To(Equal(backupTargetPVCRemoveVolumeSubmitted))
		})
	})

	Context("Error handling for utility volumes operations", func() {
		It("should handle patch errors during attach", func() {
			vmiInterface.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("attach patch failed"))

			syncInfo := backupController.attachBackupTargetPVC(testVMI, "test-pvc")
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.err).NotTo(BeNil())
			Expect(syncInfo.err.Error()).To(ContainSubstring("attach patch failed"))
		})

		It("should handle patch errors during detach", func() {
			testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{
				{
					Name: backupTargetPVC,
					PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: "test-pvc",
					},
					Type: pointer.P(v1.Backup),
				},
			}

			vmiInterface.EXPECT().Patch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("detach patch failed"))

			syncInfo := backupController.detachBackupTargetPVC(testVMI, "test-pvc")
			Expect(syncInfo).NotTo(BeNil())
			Expect(syncInfo.err).NotTo(BeNil())
			Expect(syncInfo.err.Error()).To(ContainSubstring("detach patch failed"))
		})
	})

	Context("checkBackupCompletion - Comprehensive Scenarios", func() {
		BeforeEach(func() {
			testVMI.Spec.Volumes = []v1.Volume{{Name: "disk1"}}
			testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{}
			testVMI.Status.VolumeStatus = nil                                      // Reset volume status
			testVMI.Status.ChangedBlockTracking = &v1.ChangedBlockTrackingStatus{} // Reset CBT status
			// Reset testBackup to ensure clean state
			testBackup.DeletionTimestamp = nil
			testBackup.Finalizers = nil
			testBackup.Status = nil
			testBackup.Spec.PvcName = pointer.P("backup-target-pvc") // Ensure PvcName is set
		})

		Context("Scenario 1: Normal Success Flow", func() {
			It("should perform cleanup before marking backup as done", func() {
				// Setup: backup completed successfully
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					Failed:         false,
					CheckpointName: "checkpoint-1",
				}

				// With phased cleanup: PVC detach happens now, but backup status removal
				// is deferred until AFTER backup is marked as done
				// No VMI patch expected during pre-completion cleanup

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupCompletedEvent))
				Expect(syncInfo.reason).To(Equal(backupCompleted))
				Expect(syncInfo.checkpointName).To(Equal("checkpoint-1"))
			})
		})

		Context("Scenario 2: Success with Warning", func() {
			It("should perform cleanup before marking backup as done with warning", func() {
				// Setup: backup completed with warning
				backupMsg := "disk snapshot partial failure"
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					Failed:         false,
					BackupMsg:      &backupMsg,
					CheckpointName: "checkpoint-1",
				}

				// No VMI patch expected during pre-completion cleanup (phased cleanup)

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupCompletedWithWarningEvent))
				Expect(syncInfo.reason).To(ContainSubstring(backupMsg))
				Expect(syncInfo.checkpointName).To(Equal("checkpoint-1"))
			})
		})

		Context("Scenario 3: Backup Failure", func() {
			It("should perform cleanup before marking backup as failed", func() {
				// Setup: backup failed
				failureMsg := "libvirt error"
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName: "test-backup",
					Completed:  true,
					Failed:     true,
					BackupMsg:  &failureMsg,
				}

				// No VMI patch expected during pre-completion cleanup (phased cleanup)

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupFailedEvent))
				Expect(syncInfo.reason).To(Equal(failureMsg))
			})
		})

		Context("Scenario 4: Backup Abort - In Progress", func() {
			It("should wait for abort to complete without cleanup", func() {
				// Setup: abort in progress
				abortStatus := string(backupv1.BackupAbortInProgress)
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:  "test-backup",
					Completed:   false,
					Failed:      true,
					AbortStatus: &abortStatus,
				}

				// No cleanup expected
				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).To(BeNil()) // Returns nil to wait
			})
		})

		Context("Scenario 5: Backup Abort - Succeeded", func() {
			It("should perform cleanup before marking backup as aborted", func() {
				// Setup: abort succeeded
				abortStatus := string(backupv1.BackupAbortSucceeded)
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:  "test-backup",
					Completed:   true,
					Failed:      true,
					AbortStatus: &abortStatus,
				}

				// No VMI patch expected during pre-completion cleanup (phased cleanup)

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupAbortedEvent))
				Expect(syncInfo.reason).To(Equal(abortStatus))
			})
		})

		Context("Scenario 6: Backup Abort - Failed", func() {
			It("should perform cleanup before marking abort as failed", func() {
				// Setup: abort failed
				abortStatus := string(backupv1.BackupAbortFailed)
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:  "test-backup",
					Completed:   true,
					Failed:      true,
					AbortStatus: &abortStatus,
				}

				// No VMI patch expected during pre-completion cleanup (phased cleanup)

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupAbortedEvent))
				Expect(syncInfo.reason).To(Equal(abortStatus))
			})
		})

		Context("Scenario 7: Backup In Progress", func() {
			It("should wait for backup to complete without cleanup", func() {
				// Setup: backup still in progress
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName: "test-backup",
					Completed:  false,
					Failed:     false,
				}

				// No cleanup expected
				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).To(BeNil()) // Returns nil to wait
			})
		})

		Context("Scenario 8: Backup Already Done", func() {
			It("should return early without cleanup", func() {
				// Setup: backup already marked as done
				testBackup.Status = &backupv1.VirtualMachineBackupStatus{
					Conditions: []backupv1.Condition{
						{
							Type:   backupv1.ConditionDone,
							Status: corev1.ConditionTrue,
						},
					},
				}
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					CheckpointName: "checkpoint-1",
				}

				// No cleanup expected - early exit
				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).To(BeNil())
			})
		})

		Context("Scenario 9: Backup Already Marked as Done", func() {
			It("should return early when backup is done", func() {
				// Setup: backup already marked as done
				testBackup.Status = &backupv1.VirtualMachineBackupStatus{
					Conditions: []backupv1.Condition{
						{
							Type:   backupv1.ConditionDone,
							Status: corev1.ConditionTrue,
						},
					},
				}
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName: "test-backup",
					Completed:  true,
				}

				// Early exit, no cleanup needed
				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).To(BeNil())
			})
		})

		Context("Scenario 10: Cleanup Failures and Retries", func() {
			It("should retry when PVC detach fails", func() {
				// Setup: backup completed, but PVC still attached
				testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{
					{
						Name: backupTargetPVC,
						PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "backup-target-pvc",
						},
						Type: pointer.P(v1.Backup),
					},
				}
				// Add VolumeStatus to indicate PVC is still attached
				testVMI.Status.VolumeStatus = []v1.VolumeStatus{
					{
						Name:          "backup-target-pvc",
						HotplugVolume: &v1.HotplugVolumeStatus{},
						Phase:         v1.VolumeReady,
					},
				}
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					CheckpointName: "checkpoint-1",
				}

				// PVC detach fails
				vmiInterface.EXPECT().Patch(
					context.Background(),
					testVMI.Name,
					k8stypes.JSONPatchType,
					gomock.Any(),
					gomock.Any(),
				).Return(nil, fmt.Errorf("detach failed"))

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.err).NotTo(BeNil())
				Expect(syncInfo.err.Error()).To(ContainSubstring("detach failed"))
				// Backup should NOT be marked as done
			})

			It("should not remove backup status during pre-completion cleanup", func() {
				// Setup: backup completed, PVC already detached
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					CheckpointName: "checkpoint-1",
				}

				// With phased cleanup, backup status removal does NOT happen during
				// pre-completion cleanup. It only happens after backup is marked as done.
				// No VMI patch expected here.

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupCompletedEvent))
				// Backup will be marked as done
			})
		})

		Context("Scenario 11: Multi-step Cleanup", func() {
			It("should initiate PVC detach and return detach event", func() {
				// Setup: backup completed, PVC still attached
				testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{
					{
						Name: backupTargetPVC,
						PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "backup-target-pvc",
						},
						Type: pointer.P(v1.Backup),
					},
				}
				// Add VolumeStatus to indicate PVC is still attached
				testVMI.Status.VolumeStatus = []v1.VolumeStatus{
					{
						Name:          "backup-target-pvc",
						HotplugVolume: &v1.HotplugVolumeStatus{},
						Phase:         v1.VolumeReady,
					},
				}
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					CheckpointName: "checkpoint-1",
				}

				// Expect PVC detach to be initiated (removal of utility volume)
				vmiInterface.EXPECT().Patch(
					context.Background(),
					testVMI.Name,
					k8stypes.JSONPatchType,
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
					patchStr := string(data)
					// Verify it's removing the utility volume
					Expect(patchStr).To(ContainSubstring("/spec/utilityVolumes"))
					return testVMI, nil
				})

				// When PVC is still attached, cleanup initiates detach but returns detach event
				// The backup will NOT be marked as done until detach completes
				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				// Cleanup returns detach event, not completion - backup must wait for detach
				Expect(syncInfo.event).To(Equal(backupTargetPVCRemoveVolumeSubmitted))
			})

			It("should complete after PVC detach finishes", func() {
				// Setup: backup completed, PVC already detached
				testVMI.Spec.UtilityVolumes = []v1.UtilityVolume{} // Already detached
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					CheckpointName: "checkpoint-1",
				}

				// With phased cleanup, backup status is NOT removed during pre-completion
				// No VMI patch expected

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupCompletedEvent))
				Expect(syncInfo.checkpointName).To(Equal("checkpoint-1"))
			})
		})

		Context("Scenario 12: Backup Deletion Flow", func() {
			It("should verify deletion is handled separately from completion", func() {
				// Setup: backup marked for deletion and already done
				now := metav1.Now()
				testBackup.DeletionTimestamp = &now
				testBackup.Finalizers = []string{vmBackupFinalizer}
				testBackup.Status = &backupv1.VirtualMachineBackupStatus{
					Conditions: []backupv1.Condition{
						{
							Type:   backupv1.ConditionDone,
							Status: corev1.ConditionTrue,
						},
					},
				}

				// Test that cleanup recognizes deletion scenario
				// Deletion cleanup happens in deletionCleanup(), not checkBackupCompletion()
				// This test just verifies the deletion path exists
				Expect(testBackup.DeletionTimestamp).NotTo(BeNil())
				Expect(isBackupDeleting(testBackup)).To(BeTrue())
			})
		})

		Context("Edge Cases", func() {
			It("should return early when backup is already marked as done", func() {
				testBackup.Status = &backupv1.VirtualMachineBackupStatus{
					Conditions: []backupv1.Condition{
						{
							Type:   backupv1.ConditionDone,
							Status: corev1.ConditionTrue,
						},
					},
				}
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName: "test-backup",
					Completed:  true,
				}

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				// Should return nil immediately - early exit
				Expect(syncInfo).To(BeNil())
			})

			It("should complete successfully with all steps", func() {
				testBackup.Status = nil
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName:     "test-backup",
					Completed:      true,
					CheckpointName: "checkpoint-1",
				}

				syncInfo := backupController.checkBackupCompletion(testBackup, testVMI)
				Expect(syncInfo).NotTo(BeNil())
				Expect(syncInfo.event).To(Equal(backupCompletedEvent))
			})

			It("should remove backup status when backup is already done", func() {
				// Setup: backup already marked as done
				testBackup.Status = &backupv1.VirtualMachineBackupStatus{
					Conditions: []backupv1.Condition{
						{
							Type:   backupv1.ConditionDone,
							Status: corev1.ConditionTrue,
						},
					},
				}
				testVMI.Status.ChangedBlockTracking.BackupStatus = &v1.VirtualMachineInstanceBackupStatus{
					BackupName: "test-backup",
				}

				// With phased cleanup: backup status IS removed when backup is already done
				vmiInterface.EXPECT().Patch(
					context.Background(),
					testVMI.Name,
					k8stypes.JSONPatchType,
					gomock.Any(),
					gomock.Any(),
				).DoAndReturn(func(ctx context.Context, name string, pt k8stypes.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*v1.VirtualMachineInstance, error) {
					patchStr := string(data)
					Expect(patchStr).To(ContainSubstring("/status/backupStatus"))
					return testVMI, nil
				})

				// Call cleanup directly since checkBackupCompletion returns early when done
				done, syncInfo := backupController.cleanup(testBackup, testVMI)
				Expect(done).To(BeTrue())
				Expect(syncInfo).To(BeNil())
			})
		})
	})
})
