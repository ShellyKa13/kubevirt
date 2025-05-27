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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupMode is the const type for the backup possible modes
type BackupMode string

const (
	// PushMode defines backup which pushes the backup output
	// to a provided PVC - this is the default behavior
	PushMode BackupMode = "Push"
)

type BackupAbortStatus string

const (
	// BackupAbortSucceeded means that the VirtualMachineInstance backup has been aborted
	BackupAbortSucceeded BackupAbortStatus = "Succeeded"
	// BackupAbortFailed means that the vmi backup has failed to be abort
	BackupAbortFailed BackupAbortStatus = "Failed"
	// BackupAbortInProgress mean that the vmi backup is aborting
	BackupAbortInProgress BackupAbortStatus = "Aborting"
)

// BackupOptions are options used to configure virtual machine backup job
type BackupOptions struct {
	BackupName      string       `json:"backupName,omitempty"`
	Mode            BackupMode   `json:"mode,omitempty"`
	BackupStartTime *metav1.Time `json:"backupStartTime,omitempty"`
	Incremental     *string      `json:"incremental,omitempty"`
	PushPath        *string      `json:"pushPath,omitempty"`
	SkipQuiesce     bool         `json:"skipQuiesce,omitempty"`
}
