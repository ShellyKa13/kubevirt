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

package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"kubevirt.io/kubevirt/pkg/libdv"
	"kubevirt.io/kubevirt/pkg/libvmi"
	backup "kubevirt.io/kubevirt/pkg/storage/cbt"

	cd "kubevirt.io/kubevirt/tests/containerdisk"
	"kubevirt.io/kubevirt/tests/framework/kubevirt"
	"kubevirt.io/kubevirt/tests/framework/matcher"
	. "kubevirt.io/kubevirt/tests/framework/matcher"
	"kubevirt.io/kubevirt/tests/libstorage"
	"kubevirt.io/kubevirt/tests/testsuite"
)

var groupName = "kubevirt.io"

var _ = Describe(SIG("Backup", func() {
	var (
		err        error
		virtClient kubecli.KubevirtClient
		vm         *v1.VirtualMachine
		vmi        *v1.VirtualMachineInstance
		vmbackup   *backupv1.VirtualMachineBackup
	)

	BeforeEach(func() {
		virtClient = kubevirt.Client()
	})

	createAndVerifyVMBackup := func(vm *v1.VirtualMachine, pvcName string) {
		vmbackup = NewBackup(vm.Name, vm.Namespace, pvcName)

		_, err := virtClient.VirtualMachineBackup(vmbackup.Namespace).Create(context.Background(), vmbackup, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		vmbackup = WaitBackupSucceeded(virtClient, vm.Namespace, vmbackup.Name)
		Expect(vmbackup.Status.Type).To(Equal(backupv1.Full))
		Expect(vmbackup.Status.CheckpointName).ToNot(BeEmpty())
	}

	FIt("Full Backup", func() {
		dv := libdv.NewDataVolume(
			libdv.WithRegistryURLSource(cd.DataVolumeImportUrlForContainerDisk(cd.ContainerDiskFedoraTestTooling)),
			libdv.WithNamespace(testsuite.GetTestNamespace(nil)),
			libdv.WithStorage(
				libdv.StorageWithVolumeSize(cd.FedoraVolumeSize),
			),
		)
		vm = libstorage.RenderVMWithDataVolumeTemplate(dv,
			libvmi.WithLabels(backup.CBTLabel),
			libvmi.WithRunStrategy(v1.RunStrategyAlways),
		)

		By(fmt.Sprintf("Creating VM %s", vm.Name))
		virtClient.VirtualMachine(vm.Namespace).Create(context.Background(), vm, metav1.CreateOptions{})
		Eventually(func() v1.ChangedBlockTrackingState {
			vm, err = virtClient.VirtualMachine(vm.Namespace).Get(context.Background(), vm.Name, metav1.GetOptions{})
			Expect(err).ShouldNot(HaveOccurred())
			return vm.Status.ChangedBlockTracking
		}, 3*time.Minute, 3*time.Second).Should(Equal(v1.ChangedBlockTrackingEnabled))

		Eventually(func() v1.ChangedBlockTrackingState {
			vmi, err = virtClient.VirtualMachineInstance(vm.Namespace).Get(context.Background(), vm.Name, metav1.GetOptions{})
			Expect(err).ShouldNot(HaveOccurred())
			return vmi.Status.ChangedBlockTracking
		}, 1*time.Minute, 3*time.Second).Should(Equal(v1.ChangedBlockTrackingEnabled))
		Eventually(matcher.ThisVMI(vmi), 12*time.Minute, 2*time.Second).Should(matcher.HaveConditionTrue(v1.VirtualMachineInstanceAgentConnected))

		targetPVC := libstorage.CreateFSPVC("target-pvc", testsuite.GetTestNamespace(vm), cd.FedoraVolumeSize, nil)
		createAndVerifyVMBackup(vm, targetPVC.Name)

	})
}))

func NewBackup(vmName, namespace, pvcName string) *backupv1.VirtualMachineBackup {
	return &backupv1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vmbackup-" + vmName,
			Namespace: namespace,
		},
		Spec: backupv1.VirtualMachineBackupSpec{
			Source: &corev1.TypedLocalObjectReference{
				APIGroup: &groupName,
				Kind:     "VirtualMachine",
				Name:     vmName,
			},
			PvcName: &pvcName,
		},
	}
}
func WaitBackupSucceeded(virtClient kubecli.KubevirtClient, namespace string, backupName string) *backupv1.VirtualMachineBackup {
	var vmbackup *backupv1.VirtualMachineBackup
	Eventually(func() *backupv1.VirtualMachineBackupStatus {
		var err error
		vmbackup, err = virtClient.VirtualMachineBackup(namespace).Get(context.Background(), backupName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		if vmbackup.Status != nil {
			for _, condition := range vmbackup.Status.Conditions {
				if strings.Contains(condition.Reason, "Failed freezing") {
					fmt.Println("FREEZE FAILED")
					time.Sleep(time.Minute * 100000)
				}
			}
		}
		return vmbackup.Status
	}, 180*time.Second, 2*time.Second).Should(gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Conditions": ContainElements(
			gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":   Equal(backupv1.ConditionDone),
				"Status": Equal(corev1.ConditionTrue),
				"Reason": ContainSubstring("Successfully completed VirtualMachineBackup")}),
			gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":   Equal(backupv1.ConditionProgressing),
				"Status": Equal(corev1.ConditionFalse)}),
		),
	})))

	return vmbackup
}
