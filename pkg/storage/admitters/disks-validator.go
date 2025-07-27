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

package admitters

import (
	"encoding/base64"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	k8sfield "k8s.io/apimachinery/pkg/util/validation/field"

	v1 "kubevirt.io/api/core/v1"

	hwutil "kubevirt.io/kubevirt/pkg/util/hardware"
	virtconfig "kubevirt.io/kubevirt/pkg/virt-config"
	"kubevirt.io/kubevirt/pkg/virt-config/featuregate"
)

const (
	maxStrLen = 256

	// Should be a power of 2
	minCustomBlockSize = 512
	maxCustomBlockSize = 2097152 // 2 MB
)

var isValidExpression = regexp.MustCompile(`^[A-Za-z0-9_.+-]+$`).MatchString

func ValidateDisks(field *k8sfield.Path, disks []v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	for idx, disk := range disks {
		causes = append(causes, validateDiskName(field, idx, disks)...)
		causes = append(causes, validateDeviceTarget(field, idx, disk)...)
		causes = append(causes, validatePciAddress(field, idx, disk)...)
		causes = append(causes, validateBootOrderValue(field, idx, disk)...)
		causes = append(causes, validateBusSupport(field, idx, disk)...)
		causes = append(causes, validateSerialNumValue(field, idx, disk)...)
		causes = append(causes, validateSerialNumLength(field, idx, disk)...)
		causes = append(causes, validateCacheMode(field, idx, disk)...)
		causes = append(causes, validateIOMode(field, idx, disk)...)
		causes = append(causes, validateErrorPolicy(field, idx, disk)...)
		// Verify disk and volume name can be a valid container name since disk
		// name can become a container name which will fail to schedule if invalid
		causes = append(causes, validateDiskNameAsContainerName(field, idx, disk)...)
		causes = append(causes, validateBlockSize(field, idx, disk)...)
	}
	return causes
}

func ValidateContainerDisks(field *k8sfield.Path, spec *v1.VirtualMachineInstanceSpec) []metav1.StatusCause {
	var causes []metav1.StatusCause
	for idx, volume := range spec.Volumes {
		if volume.ContainerDisk == nil || volume.ContainerDisk.Path == "" {
			continue
		}
		causes = append(causes, ValidatePath(field.Child("volumes").Index(idx).Child("containerDisk"), volume.ContainerDisk.Path)...)
	}
	return causes
}

func validateDiskName(field *k8sfield.Path, idx int, disks []v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	for otherIdx, disk := range disks {
		if otherIdx < idx && disk.Name == disks[idx].Name {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s and %s must not have the same Name.", field.Index(idx).String(), field.Index(otherIdx).String()),
				Field:   field.Index(idx).Child("name").String(),
			})
		}
	}
	return causes
}

func validateDeviceTarget(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	deviceTargetSetCount := 0
	if disk.Disk != nil {
		deviceTargetSetCount++
	}
	if disk.LUN != nil {
		deviceTargetSetCount++
	}
	if disk.CDRom != nil {
		deviceTargetSetCount++
	}
	// NOTE: not setting a device target is okay. We default to Disk.
	// However, only a single device target is allowed to be set at a time.
	if deviceTargetSetCount > 1 {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s can only have a single target type defined", field.Index(idx).String()),
			Field:   field.Index(idx).String(),
		})
	}
	return causes
}

func validatePciAddress(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.Disk == nil || disk.Disk.PciAddress == "" {
		return causes
	}

	if disk.Disk.Bus != v1.DiskBusVirtio {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("disk %s - setting a PCI address is only possible with bus type virtio.", field.Child("domain", "devices", "disks", "disk").Index(idx).Child("name").String()),
			Field:   field.Child("domain", "devices", "disks", "disk").Index(idx).Child("pciAddress").String(),
		})
	}

	if _, err := hwutil.ParsePciAddress(disk.Disk.PciAddress); err != nil {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("disk %s has malformed PCI address (%s).", field.Child("domain", "devices", "disks", "disk").Index(idx).Child("name").String(), disk.Disk.PciAddress),
			Field:   field.Child("domain", "devices", "disks", "disk").Index(idx).Child("pciAddress").String(),
		})
	}
	return causes
}

func validateBootOrderValue(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.BootOrder != nil && *disk.BootOrder < 1 {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must have a boot order > 0, if supplied", field.Index(idx).String()),
			Field:   field.Index(idx).Child("bootOrder").String(),
		})
	}
	return causes
}

func getDiskBus(disk v1.Disk) v1.DiskBus {
	switch {
	case disk.Disk != nil:
		return disk.Disk.Bus
	case disk.LUN != nil:
		return disk.LUN.Bus
	case disk.CDRom != nil:
		return disk.CDRom.Bus
	default:
		return ""
	}
}

func getDiskType(disk v1.Disk) string {
	switch {
	case disk.Disk != nil:
		return "disk"
	case disk.LUN != nil:
		return "lun"
	case disk.CDRom != nil:
		return "cdrom"
	default:
		return ""
	}
}

func validateBusSupport(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	bus := getDiskBus(disk)
	diskType := getDiskType(disk)
	if bus == "" {
		return causes
	}
	switch bus {
	case "ide":
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: "IDE bus is not supported",
			Field:   field.Index(idx).Child(diskType, "bus").String(),
		})
	case v1.DiskBusVirtio:
		// special case. virtio is incompatible with CD-ROM for q35 machine types
		if diskType == "cdrom" {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("Bus type %s is invalid for CD-ROM device", bus),
				Field:   field.Index(idx).Child("cdrom", "bus").String(),
			})
		}
	case v1.DiskBusSATA:
		// sata disks (in contrast to sata cdroms) don't support readOnly
		if disk.Disk != nil && disk.Disk.ReadOnly {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s hard-disks do not support read-only.", bus),
				Field:   field.Index(idx).Child("disk", "bus").String(),
			})
		}
	case v1.DiskBusSCSI, v1.DiskBusUSB:
		break
	default:
		supportedBuses := []v1.DiskBus{v1.DiskBusVirtio, v1.DiskBusSCSI, v1.DiskBusSATA, v1.DiskBusUSB}
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s is set with an unrecognized bus %s, must be one of: %v", field.Index(idx).String(), bus, supportedBuses),
			Field:   field.Index(idx).Child(diskType, "bus").String(),
		})
	}
	// Reject defining DedicatedIOThread to a disk without VirtIO bus since this configuration
	// is not supported in libvirt.
	if disk.DedicatedIOThread != nil && *disk.DedicatedIOThread && bus != v1.DiskBusVirtio {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueNotSupported,
			Message: fmt.Sprintf("IOThreads are not supported for disks on a %s bus", bus),
			Field:   field.Child("domain", "devices", "disks").Index(idx).String(),
		})
	}
	return causes
}

func validateSerialNumValue(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.Serial != "" && !isValidExpression(disk.Serial) {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must be made up of the following characters [A-Za-z0-9_.+-], if specified", field.Index(idx).String()),
			Field:   field.Index(idx).Child("serial").String(),
		})
	}
	return causes
}

func validateSerialNumLength(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.Serial != "" && len([]rune(disk.Serial)) > maxStrLen {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must be less than or equal to %d in length, if specified", field.Index(idx).String(), maxStrLen),
			Field:   field.Index(idx).Child("serial").String(),
		})
	}
	return causes
}

func validateCacheMode(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.Cache != "" && disk.Cache != v1.CacheNone && disk.Cache != v1.CacheWriteThrough && disk.Cache != v1.CacheWriteBack {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s has invalid value %s", field.Index(idx).Child("cache").String(), disk.Cache),
			Field:   field.Index(idx).Child("cache").String(),
		})
	}
	return causes
}

func validateIOMode(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.IO != "" && disk.IO != v1.IONative && disk.IO != v1.IOThreads {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueNotSupported,
			Message: fmt.Sprintf("Disk IO mode for %s is not supported. Supported modes are: native, threads.", field),
			Field:   field.Child("domain", "devices", "disks").Index(idx).Child("io").String(),
		})
	}
	return causes
}

func validateErrorPolicy(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.ErrorPolicy != nil && *disk.ErrorPolicy != v1.DiskErrorPolicyStop && *disk.ErrorPolicy != v1.DiskErrorPolicyIgnore && *disk.ErrorPolicy != v1.DiskErrorPolicyReport && *disk.ErrorPolicy != v1.DiskErrorPolicyEnospace {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s has invalid value \"%s\"", field.Index(idx).Child("errorPolicy").String(), *disk.ErrorPolicy),
			Field:   field.Index(idx).Child("errorPolicy").String(),
		})
	}
	return causes
}

func validateDiskNameAsContainerName(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	for _, err := range validation.IsDNS1123Label(disk.Name) {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: err,
			Field:   field.Child("domain", "devices", "disks").Index(idx).Child("name").String(),
		})
	}
	return causes
}

func validateCustomBlockSize(field *k8sfield.Path, idx int, blockType string, size uint) []metav1.StatusCause {
	var causes []metav1.StatusCause
	switch {
	case size < minCustomBlockSize:
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("Provided size of %d is less than the supported minimum size of %d", size, minCustomBlockSize),
			Field:   field.Index(idx).Child("blockSize").Child("custom").Child(blockType).String(),
		})
	case size > maxCustomBlockSize:
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("Provided size of %d is greater than the supported maximum size of %d", size, maxCustomBlockSize),
			Field:   field.Index(idx).Child("blockSize").Child("custom").Child(blockType).String(),
		})
	case size&(size-1) != 0:
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("Provided size of %d is not a power of 2", size),
			Field:   field.Index(idx).Child("blockSize").Child("custom").Child(blockType).String(),
		})
	}
	return causes
}

func validateBlockSize(field *k8sfield.Path, idx int, disk v1.Disk) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if disk.BlockSize == nil || disk.BlockSize.Custom == nil {
		return causes
	}
	if disk.BlockSize.MatchVolume != nil && (disk.BlockSize.MatchVolume.Enabled == nil || *disk.BlockSize.MatchVolume.Enabled) {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: "Block size matching can't be enabled together with a custom value",
			Field:   field.Index(idx).Child("blockSize").String(),
		})
		return causes
	}
	customSize := disk.BlockSize.Custom
	if customSize.Logical > customSize.Physical {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("Logical size %d must be the same or less than the physical size of %d", customSize.Logical, customSize.Physical),
			Field:   field.Index(idx).Child("blockSize").Child("custom").Child("logical").String(),
		})
	} else {
		causes = append(causes, validateCustomBlockSize(field, idx, "logical", customSize.Logical)...)
		causes = append(causes, validateCustomBlockSize(field, idx, "physical", customSize.Physical)...)
	}
	return causes
}

func ValidatePath(field *k8sfield.Path, path string) []metav1.StatusCause {
	var causes []metav1.StatusCause
	if path == "/" {
		causes = append(causes, metav1.StatusCause{
			Type: metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must not point to root",
				field.String(),
			),
			Field: field.String(),
		})
		return causes
	}
	cleanedPath := filepath.Join("/", path)
	providedPath := strings.TrimSuffix(path, "/") // Join trims suffix slashes

	if cleanedPath != providedPath {
		causes = append(causes, metav1.StatusCause{
			Type: metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must be an absolute path to a file without relative components",
				field.String(),
			),
			Field: field.String(),
		})
	}

	return causes
}

func validateBootOrder(field *k8sfield.Path, spec *v1.VirtualMachineInstanceSpec, config *virtconfig.ClusterConfig) []metav1.StatusCause {
	var causes []metav1.StatusCause
	// used to validate uniqueness of boot orders among disks and interfaces
	bootOrderMap := make(map[uint]bool)
	volumeNameMap := make(map[string]*v1.Volume)

	for i, volume := range spec.Volumes {
		volumeNameMap[volume.Name] = &spec.Volumes[i]
	}

	// Validate disks match volumes correctly
	for idx, disk := range spec.Domain.Devices.Disks {
		var matchingVolume *v1.Volume

		matchingVolume, volumeExists := volumeNameMap[disk.Name]

		if !volumeExists {
			if disk.CDRom == nil {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf(nameOfTypeNotFoundMessagePattern, field.Child("domain", "devices", "disks").Index(idx).Child("Name").String(), disk.Name),
					Field:   field.Child("domain", "devices", "disks").Index(idx).Child("name").String(),
				})
			} else if !config.DeclarativeHotplugVolumesEnabled() {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s feature gate not enabled, cannot define an empty CD-ROM disk", featuregate.DeclarativeHotplugVolumesGate),
					Field:   field.Child("domain", "devices", "disks").Index(idx).Child("name").String(),
				})
			}
		}

		// Verify Lun disks are only mapped to network/block devices.
		if disk.LUN != nil && volumeExists && matchingVolume.PersistentVolumeClaim == nil && matchingVolume.DataVolume == nil {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s can only be mapped to a DataVolume or PersistentVolumeClaim volume.", field.Child("domain", "devices", "disks").Index(idx).Child("lun").String()),
				Field:   field.Child("domain", "devices", "disks").Index(idx).Child("lun").String(),
			})
		}

		// Verify that DownwardMetrics is mapped to disk
		if volumeExists && matchingVolume.DownwardMetrics != nil {
			if disk.Disk == nil {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueRequired,
					Message: fmt.Sprintf("DownwardMetrics volume must be mapped to a disk, but disk is not set on %v.", field.Child("domain", "devices", "disks").Index(idx).Child("disk").String()),
					Field:   field.Child("domain", "devices", "disks").Index(idx).Child("disk").String(),
				})
			} else if disk.Disk != nil && disk.Disk.Bus != v1.DiskBusVirtio && disk.Disk.Bus != "" {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("DownwardMetrics volume must be mapped to virtio bus, but %v is set to %v", field.Child("domain", "devices", "disks").Index(idx).Child("disk").Child("bus").String(), disk.Disk.Bus),
					Field:   field.Child("domain", "devices", "disks").Index(idx).Child("disk").Child("bus").String(),
				})
			}
		}

		// verify that there are no duplicate boot orders
		if disk.BootOrder != nil {
			order := *disk.BootOrder
			if bootOrderMap[order] {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("Boot order for %s already set for a different device.", field.Child("domain", "devices", "disks").Index(idx).Child("bootOrder").String()),
					Field:   field.Child("domain", "devices", "disks").Index(idx).Child("bootOrder").String(),
				})
			}
			bootOrderMap[order] = true
		}
	}

	causes = append(causes, validateInterfaceBootOrder(field, spec, bootOrderMap)...)

	return causes
}

func ValidateHotplugDiskConfiguration(disk *v1.Disk, name, messagePrefix, field string) []metav1.StatusCause {
	if disk == nil {
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s for [%s] requires the disk field to be set.", messagePrefix, name),
			Field:   field,
		}}
	}

	bus := getDiskBus(*disk)
	switch {
	case disk.DiskDevice.Disk != nil:
		if bus != v1.DiskBusSCSI && bus != v1.DiskBusVirtio {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s for disk [%s] requires bus to be 'scsi' or 'virtio'. [%s] is not permitted.", messagePrefix, name, bus),
				Field:   field,
			}}
		}
	case disk.DiskDevice.LUN != nil:
		if bus != v1.DiskBusSCSI {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s for LUN [%s] requires bus to be 'scsi'. [%s] is not permitted.", messagePrefix, name, bus),
				Field:   field,
			}}
		}
	default:
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s for [%s] requires diskDevice of type 'disk' or 'lun' to be used.", messagePrefix, name),
			Field:   field,
		}}
	}

	if disk.DedicatedIOThread != nil && *disk.DedicatedIOThread && bus != v1.DiskBusVirtio {
		return []metav1.StatusCause{{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s for [%s] requires virtio bus for IOThreads.", messagePrefix, name),
			Field:   field,
		}}
	}

	// Validate boot order
	if disk.BootOrder != nil {
		order := *disk.BootOrder
		if order < 1 {
			return []metav1.StatusCause{{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("spec.domain.devices.disks[1] must have a boot order > 0, if supplied"),
				Field:   fmt.Sprintf("spec.domain.devices.disks[1].bootOrder"),
			}}
		}
	}

	return nil
}

func validateVolumes(field *k8sfield.Path, volumes []v1.Volume, config *virtconfig.ClusterConfig) []metav1.StatusCause {
	var causes []metav1.StatusCause
	nameMap := make(map[string]int)

	// check that we have max 1 instance of below disks
	serviceAccountVolumeCount := 0
	downwardMetricVolumeCount := 0
	memoryDumpVolumeCount := 0

	for idx, volume := range volumes {
		// verify name is unique
		otherIdx, ok := nameMap[volume.Name]
		if !ok {
			nameMap[volume.Name] = idx
		} else {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s and %s must not have the same Name.", field.Index(idx).String(), field.Index(otherIdx).String()),
				Field:   field.Index(idx).Child("name").String(),
			})
		}

		// Verify exactly one source is set
		volumeSourceSetCount := 0
		if volume.PersistentVolumeClaim != nil {
			volumeSourceSetCount++
		}
		if volume.Sysprep != nil {
			volumeSourceSetCount++
		}
		if volume.CloudInitNoCloud != nil {
			volumeSourceSetCount++
		}
		if volume.CloudInitConfigDrive != nil {
			volumeSourceSetCount++
		}
		if volume.ContainerDisk != nil {
			volumeSourceSetCount++
		}
		if volume.Ephemeral != nil {
			volumeSourceSetCount++
		}
		if volume.EmptyDisk != nil {
			volumeSourceSetCount++
		}
		if volume.HostDisk != nil {
			volumeSourceSetCount++
		}
		if volume.DataVolume != nil {
			if volume.DataVolume.Name == "" {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueRequired,
					Message: "DataVolume 'name' must be set",
					Field:   field.Index(idx).Child("name").String(),
				})
			}
			volumeSourceSetCount++
		}
		if volume.ConfigMap != nil {
			volumeSourceSetCount++
		}
		if volume.Secret != nil {
			volumeSourceSetCount++
		}
		if volume.DownwardAPI != nil {
			volumeSourceSetCount++
		}
		if volume.ServiceAccount != nil {
			volumeSourceSetCount++
			serviceAccountVolumeCount++
		}
		if volume.DownwardMetrics != nil {
			downwardMetricVolumeCount++
			volumeSourceSetCount++
		}
		if volume.MemoryDump != nil {
			memoryDumpVolumeCount++
			volumeSourceSetCount++
		}

		if volumeSourceSetCount != 1 {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: fmt.Sprintf("%s must have exactly one source type set", field.Index(idx).String()),
				Field:   field.Index(idx).String(),
			})
		}

		// Verify cloud init data is within size limits
		if volume.CloudInitNoCloud != nil || volume.CloudInitConfigDrive != nil {
			var userDataSecretRef, networkDataSecretRef *k8sv1.LocalObjectReference
			var dataSourceType, userData, userDataBase64, networkData, networkDataBase64 string
			if volume.CloudInitNoCloud != nil {
				dataSourceType = "cloudInitNoCloud"
				userDataSecretRef = volume.CloudInitNoCloud.UserDataSecretRef
				userDataBase64 = volume.CloudInitNoCloud.UserDataBase64
				userData = volume.CloudInitNoCloud.UserData
				networkDataSecretRef = volume.CloudInitNoCloud.NetworkDataSecretRef
				networkDataBase64 = volume.CloudInitNoCloud.NetworkDataBase64
				networkData = volume.CloudInitNoCloud.NetworkData
			} else if volume.CloudInitConfigDrive != nil {
				dataSourceType = "cloudInitConfigDrive"
				userDataSecretRef = volume.CloudInitConfigDrive.UserDataSecretRef
				userDataBase64 = volume.CloudInitConfigDrive.UserDataBase64
				userData = volume.CloudInitConfigDrive.UserData
				networkDataSecretRef = volume.CloudInitConfigDrive.NetworkDataSecretRef
				networkDataBase64 = volume.CloudInitConfigDrive.NetworkDataBase64
				networkData = volume.CloudInitConfigDrive.NetworkData
			}

			userDataLen := 0
			userDataSourceCount := 0
			networkDataLen := 0
			networkDataSourceCount := 0

			if userDataSecretRef != nil && userDataSecretRef.Name != "" {
				userDataSourceCount++
			}
			if userDataBase64 != "" {
				userDataSourceCount++
				userData, err := base64.StdEncoding.DecodeString(userDataBase64)
				if err != nil {
					causes = append(causes, metav1.StatusCause{
						Type:    metav1.CauseTypeFieldValueInvalid,
						Message: fmt.Sprintf("%s.%s.userDataBase64 is not a valid base64 value.", field.Index(idx).Child(dataSourceType, "userDataBase64").String(), dataSourceType),
						Field:   field.Index(idx).Child(dataSourceType, "userDataBase64").String(),
					})
				}
				userDataLen = len(userData)
			}
			if userData != "" {
				userDataSourceCount++
				userDataLen = len(userData)
			}

			if userDataSourceCount > 1 {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s must have only one userdatasource set.", field.Index(idx).Child(dataSourceType).String()),
					Field:   field.Index(idx).Child(dataSourceType).String(),
				})
			}

			if userDataLen > cloudInitUserMaxLen {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s userdata exceeds %d byte limit. Should use UserDataSecretRef for larger data.", field.Index(idx).Child(dataSourceType).String(), cloudInitUserMaxLen),
					Field:   field.Index(idx).Child(dataSourceType).String(),
				})
			}

			if networkDataSecretRef != nil && networkDataSecretRef.Name != "" {
				networkDataSourceCount++
			}
			if networkDataBase64 != "" {
				networkDataSourceCount++
				networkData, err := base64.StdEncoding.DecodeString(networkDataBase64)
				if err != nil {
					causes = append(causes, metav1.StatusCause{
						Type:    metav1.CauseTypeFieldValueInvalid,
						Message: fmt.Sprintf("%s.%s.networkDataBase64 is not a valid base64 value.", field.Index(idx).Child(dataSourceType, "networkDataBase64").String(), dataSourceType),
						Field:   field.Index(idx).Child(dataSourceType, "networkDataBase64").String(),
					})
				}
				networkDataLen = len(networkData)
			}
			if networkData != "" {
				networkDataSourceCount++
				networkDataLen = len(networkData)
			}

			if networkDataSourceCount > 1 {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s must have only one networkdata source set.", field.Index(idx).Child(dataSourceType).String()),
					Field:   field.Index(idx).Child(dataSourceType).String(),
				})
			}

			if networkDataLen > cloudInitNetworkMaxLen {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s networkdata exceeds %d byte limit. Should use NetworkDataSecretRef for larger data.", field.Index(idx).Child(dataSourceType).String(), cloudInitNetworkMaxLen),
					Field:   field.Index(idx).Child(dataSourceType).String(),
				})
			}

			if userDataSourceCount == 0 && networkDataSourceCount == 0 {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s must have at least one userdatasource or one networkdatasource set.", field.Index(idx).Child(dataSourceType).String()),
					Field:   field.Index(idx).Child(dataSourceType).String(),
				})
			}
		}

		if volume.DownwardMetrics != nil && !config.DownwardMetricsEnabled() {
			causes = append(causes, metav1.StatusCause{
				Type:    metav1.CauseTypeFieldValueInvalid,
				Message: "downwardMetrics disks are not allowed: DownwardMetrics feature gate is not enabled.",
				Field:   field.Index(idx).String(),
			})
		}

		// validate HostDisk data
		if hostDisk := volume.HostDisk; hostDisk != nil {
			if !config.HostDiskEnabled() {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: "HostDisk feature gate is not enabled",
					Field:   field.Index(idx).String(),
				})
			}
			if hostDisk.Path == "" {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueNotFound,
					Message: fmt.Sprintf("%s is required for hostDisk volume", field.Index(idx).Child("hostDisk", "path").String()),
					Field:   field.Index(idx).Child("hostDisk", "path").String(),
				})
			}

			if hostDisk.Type != v1.HostDiskExists && hostDisk.Type != v1.HostDiskExistsOrCreate {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s has invalid value '%s', allowed are '%s' or '%s'", field.Index(idx).Child("hostDisk", "type").String(), hostDisk.Type, v1.HostDiskExists, v1.HostDiskExistsOrCreate),
					Field:   field.Index(idx).Child("hostDisk", "type").String(),
				})
			}

			// if disk.img already exists and user knows that by specifying type 'Disk' it is pointless to set capacity
			if hostDisk.Type == v1.HostDiskExists && !hostDisk.Capacity.IsZero() {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf("%s is allowed to pass only with %s equal to '%s'", field.Index(idx).Child("hostDisk", "capacity").String(), field.Index(idx).Child("hostDisk", "type").String(), v1.HostDiskExistsOrCreate),
					Field:   field.Index(idx).Child("hostDisk", "capacity").String(),
				})
			}
		}

		if volume.ConfigMap != nil {
			if volume.ConfigMap.LocalObjectReference.Name == "" {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf(requiredFieldFmt, field.Index(idx).Child("configMap", "name").String()),
					Field:   field.Index(idx).Child("configMap", "name").String(),
				})
			}
		}

		if volume.Secret != nil {
			if volume.Secret.SecretName == "" {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf(requiredFieldFmt, field.Index(idx).Child("secret", "secretName").String()),
					Field:   field.Index(idx).Child("secret", "secretName").String(),
				})
			}
		}

		if volume.ServiceAccount != nil {
			if volume.ServiceAccount.ServiceAccountName == "" {
				causes = append(causes, metav1.StatusCause{
					Type:    metav1.CauseTypeFieldValueInvalid,
					Message: fmt.Sprintf(requiredFieldFmt, field.Index(idx).Child("serviceAccount", "serviceAccountName").String()),
					Field:   field.Index(idx).Child("serviceAccount", "serviceAccountName").String(),
				})
			}
		}
	}

	if serviceAccountVolumeCount > 1 {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must have max one serviceAccount volume set", field.String()),
			Field:   field.String(),
		})
	}

	if downwardMetricVolumeCount > 1 {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must have max one downwardMetric volume set", field.String()),
			Field:   field.String(),
		})
	}
	if memoryDumpVolumeCount > 1 {
		causes = append(causes, metav1.StatusCause{
			Type:    metav1.CauseTypeFieldValueInvalid,
			Message: fmt.Sprintf("%s must have max one memory dump volume set", field.String()),
			Field:   field.String(),
		})
	}

	return causes
}
