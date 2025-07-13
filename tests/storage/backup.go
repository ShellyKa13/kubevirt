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
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1 "kubevirt.io/api/backup/v1alpha1"
	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"

	"kubevirt.io/kubevirt/pkg/libdv"
	"kubevirt.io/kubevirt/pkg/libvmi"
	backup "kubevirt.io/kubevirt/pkg/storage/cbt"

	cd "kubevirt.io/kubevirt/tests/containerdisk"
	"kubevirt.io/kubevirt/tests/events"
	"kubevirt.io/kubevirt/tests/exec"
	"kubevirt.io/kubevirt/tests/framework/kubevirt"
	"kubevirt.io/kubevirt/tests/framework/matcher"
	"kubevirt.io/kubevirt/tests/libstorage"
	"kubevirt.io/kubevirt/tests/testsuite"
)

var groupName = "kubevirt.io"

var _ = Describe(SIG("Backup", func() {
	var (
		err        error
		virtClient kubecli.KubevirtClient
		vm         *v1.VirtualMachine
	)

	BeforeEach(func() {
		virtClient = kubevirt.Client()
	})

	createAndVerifyVMBackup := func(vm *v1.VirtualMachine, pvcName string) {
		vmbackup := NewBackup(vm.Name, vm.Namespace, pvcName)

		_, err := virtClient.VirtualMachineBackup(vmbackup.Namespace).Create(context.Background(), vmbackup, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())

		vmbackup = waitBackupSucceeded(virtClient, vm.Namespace, vmbackup.Name)
		Expect(vmbackup.Status.Type).To(Equal(backupv1.Full))
		Expect(vmbackup.Status.CheckpointName).ToNot(BeEmpty())
	}

	DescribeTable("Full Backup", func(pvcSize string, expectedBackupCount int, shouldFail bool) {
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
		vm, err = virtClient.VirtualMachine(vm.Namespace).Create(context.Background(), vm, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
		Eventually(matcher.ThisVMIWith(vm.Namespace, vm.Name), 12*time.Minute, 2*time.Second).Should(matcher.HaveConditionTrue(v1.VirtualMachineInstanceAgentConnected))
		libstorage.WaitForCBTEnabled(virtClient, vm.Namespace, vm.Name)

		targetPVC := libstorage.CreateFSPVC("target-pvc", testsuite.GetTestNamespace(vm), pvcSize, libstorage.WithStorageProfile())

		createVMBackup(virtClient, vm, targetPVC.Name)
		if shouldFail {
			waitBackupFailed(virtClient, vm.Namespace, backupName(vm.Name))
		} else {
			By("Creating the backup")
			vmbackup := waitBackupSucceeded(virtClient, vm.Namespace, backupName(vm.Name))
			Expect(vmbackup.Status.Type).To(Equal(backupv1.Full))
			if expectedBackupCount > 1 {
				By("Deleting the backup")
				deleteVMBackup(virtClient, vm.Namespace, backupName(vm.Name))
				By("Creating another backup")
				createAndVerifyVMBackup(vm, targetPVC.Name)
			}
		}
		verifyBackupTargetPVCOutput(virtClient, targetPVC, vm.Name, expectedBackupCount)
	},
		FEntry("should succeed", getTargetPVCSizeWithOverhead(cd.FedoraVolumeSize), 1, false),
		Entry("2 backups to the same PVC should succeed", getDoubleTargetPVCSize(cd.FedoraVolumeSize), 2, false),
		Entry("with small PVC size should fail", cd.FedoraVolumeSize, 0, true),
	)
}))

func backupName(vmName string) string {
	return "vmbackup-" + vmName
}

func NewBackup(vmName, namespace, pvcName string) *backupv1.VirtualMachineBackup {
	return &backupv1.VirtualMachineBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backupName(vmName),
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

func createVMBackup(virtClient kubecli.KubevirtClient, vm *v1.VirtualMachine, pvcName string) {
	vmbackup := NewBackup(vm.Name, vm.Namespace, pvcName)
	_, err := virtClient.VirtualMachineBackup(vmbackup.Namespace).Create(context.Background(), vmbackup, metav1.CreateOptions{})
	Expect(err).ToNot(HaveOccurred())
}

func deleteVMBackup(virtClient kubecli.KubevirtClient, namespace string, backupName string) {
	err := virtClient.VirtualMachineBackup(namespace).Delete(context.Background(), backupName, metav1.DeleteOptions{})
	Expect(err).ToNot(HaveOccurred())
	Eventually(func() error {
		_, err := virtClient.VirtualMachineBackup(namespace).Get(context.Background(), backupName, metav1.GetOptions{})
		return err
	}, 180*time.Second, 2*time.Second).Should(MatchError(errors.IsNotFound, "k8serrors.IsNotFound"))
}

func createAndVerifyVMBackup(virtClient kubecli.KubevirtClient, vm *v1.VirtualMachine, pvcName string) {
	createVMBackup(virtClient, vm, pvcName)
	vmbackup := waitBackupSucceeded(virtClient, vm.Namespace, backupName(vm.Name))
	Expect(vmbackup.Status.Type).To(Equal(backupv1.Full))
}

func waitBackupSucceeded(virtClient kubecli.KubevirtClient, namespace string, backupName string) *backupv1.VirtualMachineBackup {
	var vmbackup *backupv1.VirtualMachineBackup
	Eventually(func() *backupv1.VirtualMachineBackupStatus {
		var err error
		vmbackup, err = virtClient.VirtualMachineBackup(namespace).Get(context.Background(), backupName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
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

	events.ExpectEvent(vmbackup, corev1.EventTypeNormal, "VirtualMachineBackupCompletedSuccessfully")
	Expect(vmbackup.Status.CheckpointName).ToNot(BeEmpty())

	return vmbackup
}

func waitBackupFailed(virtClient kubecli.KubevirtClient, namespace string, backupName string) *backupv1.VirtualMachineBackup {
	var vmbackup *backupv1.VirtualMachineBackup
	Eventually(func() *backupv1.VirtualMachineBackupStatus {
		var err error
		vmbackup, err = virtClient.VirtualMachineBackup(namespace).Get(context.Background(), backupName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		return vmbackup.Status
	}, 180*time.Second, 2*time.Second).Should(gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Conditions": ContainElements(
			gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":   Equal(backupv1.ConditionDone),
				"Status": Equal(corev1.ConditionTrue),
				"Reason": ContainSubstring("Failed to create VirtualMachineBackup")}),
			gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":   Equal(backupv1.ConditionFailure),
				"Status": Equal(corev1.ConditionTrue),
				"Reason": ContainSubstring("No space left on device")}),
			gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Type":   Equal(backupv1.ConditionProgressing),
				"Status": Equal(corev1.ConditionFalse)}),
		),
	})))

	events.ExpectEvent(vmbackup, corev1.EventTypeWarning, "VirtualMachineBackupFailed")
	Expect(vmbackup.Status.CheckpointName).To(BeEmpty())

	return vmbackup
}

func getTargetPVCSizeWithOverhead(originalSize string) string {
	originalQuantity := resource.MustParse(originalSize)
	smallerQuantity := originalQuantity.DeepCopy()
	smallerQuantity.Set(int64(float64(originalQuantity.Value()) * 1.2))
	return smallerQuantity.String()
}

func getDoubleTargetPVCSize(originalSize string) string {
	originalQuantity := resource.MustParse(originalSize)
	smallerQuantity := originalQuantity.DeepCopy()
	smallerQuantity.Set(int64(float64(originalQuantity.Value()) * 2.2))
	return smallerQuantity.String()
}

func createExecutorPod(virtClient kubecli.KubevirtClient, targetPVC *corev1.PersistentVolumeClaim) *corev1.Pod {
	pod := libstorage.RenderPodWithPVC("verifier", []string{"/bin/bash", "-c", "touch /tmp/startup; while true; do echo hello; sleep 2; done"}, nil, targetPVC)
	pod.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"/bin/cat", "/tmp/startup"},
			},
		},
	}
	return runPodAndExpectPhase(pod, corev1.PodRunning)
}

func verifyBackupTargetPVCOutput(virtClient kubecli.KubevirtClient, targetPVC *corev1.PersistentVolumeClaim, vmName string, numBackupFiles int) {
	executorPod := createExecutorPod(virtClient, targetPVC)

	backupOutputPath := fmt.Sprintf("%s/%s", libstorage.DefaultPvcMountPath, vmName)

	lsOutput, err := exec.ExecuteCommandOnPod(
		executorPod,
		executorPod.Spec.Containers[0].Name,
		[]string{"/bin/sh", "-c", fmt.Sprintf("ls -1 %s", backupOutputPath)},
	)
	Expect(err).ToNot(HaveOccurred())

	lsOutput = strings.TrimSpace(lsOutput)
	var lsOutputList []string
	if lsOutput == "" {
		fmt.Println("lsOutput is empty")
		lsOutputList = []string{}
	} else {
		lsOutputList = strings.Split(lsOutput, "\n")
	}

	Expect(lsOutputList).To(HaveLen(numBackupFiles))
	for _, file := range lsOutputList {
		Expect(file).To(ContainSubstring(backupName(vmName)))
	}

	Eventually(func() error {
		return virtClient.CoreV1().Pods(executorPod.Namespace).Delete(context.Background(), executorPod.Name, metav1.DeleteOptions{})
	}, 180*time.Second, time.Second).Should(MatchError(errors.IsNotFound, "k8serrors.IsNotFound"))
}
