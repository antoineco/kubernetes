/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/compute"
	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/types"
	kwait "k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/util/keymutex"
)

const (
	storageAccountNameTemplate = "pvc%s"

	// for limits check https://docs.microsoft.com/en-us/azure/azure-subscription-service-limits#storage-limits
	maxStorageAccounts                     = 100 // max # is 200 (250 with special request). this allows 100 for everything else including stand alone disks
	maxDisksPerStorageAccounts             = 60
	storageAccountUtilizationBeforeGrowing = 0.5

	maxLUN               = 64 // max number of LUNs per VM
	errLeaseFailed       = "AcquireDiskLeaseFailed"
	errLeaseIDMissing    = "LeaseIdMissing"
	errContainerNotFound = "ContainerNotFound"
	errDiskBlobNotFound  = "DiskBlobNotFound"
)

var defaultBackOff = kwait.Backoff{
	Steps:    20,
	Duration: 2 * time.Second,
	Factor:   1.5,
	Jitter:   0.0,
}

// acquire lock to attach/detach disk in one node
var diskOpMutex = keymutex.NewKeyMutex()

type controllerCommon struct {
	subscriptionID        string
	location              string
	storageEndpointSuffix string
	resourceGroup         string
	cloud                 *Cloud
}

// AttachDisk attaches a vhd to vm. The vhd must exist, can be identified by diskName, diskURI.
func (c *controllerCommon) AttachDisk(isManagedDisk bool, diskName, diskURI string, nodeName types.NodeName, cachingMode compute.CachingTypes) error {
	// 1. vmType is standard, attach with availabilitySet.AttachDisk.
	if c.cloud.VMType == vmTypeStandard {
		instanceid, err := c.cloud.InstanceID(context.TODO(), nodeName)
		if err != nil {
			glog.Warningf("failed to get azure instance id (%v)", err)
			return fmt.Errorf("failed to get azure instance id for node %q (%v)", nodeName, err)
		}

		diskOpMutex.LockKey(instanceid)
		defer diskOpMutex.UnlockKey(instanceid)

		lun, err := c.GetNextDiskLun(nodeName)
		if err != nil {
			glog.Warningf("no LUN available for instance %q (%v)", nodeName, err)
			return fmt.Errorf("all LUNs are used, cannot attach volume (%s, %s) to instance %q (%v)", diskName, diskURI, instanceid, err)
		}

		return c.cloud.vmSet.AttachDisk(isManagedDisk, diskName, diskURI, nodeName, lun, cachingMode)
	}

	// 2. vmType is Virtual Machine Scale Set (vmss), convert vmSet to scaleSet.
	ss, ok := c.cloud.vmSet.(*scaleSet)
	if !ok {
		return fmt.Errorf("error of converting vmSet (%q) to scaleSet with vmType %q", c.cloud.vmSet, c.cloud.VMType)
	}

	// 3. If the node is managed by availability set, then attach with availabilitySet.AttachDisk.
	managedByAS, err := ss.isNodeManagedByAvailabilitySet(mapNodeNameToVMName(nodeName))
	if err != nil {
		return err
	}
	if managedByAS {
		instanceid, err := c.cloud.InstanceID(context.TODO(), nodeName)
		if err != nil {
			glog.Warningf("failed to get azure instance id (%v)", err)
			return fmt.Errorf("failed to get azure instance id for node %q (%v)", nodeName, err)
		}

		diskOpMutex.LockKey(instanceid)
		defer diskOpMutex.UnlockKey(instanceid)

		lun, err := c.GetNextDiskLun(nodeName)
		if err != nil {
			glog.Warningf("no LUN available for instance %q (%v)", nodeName, err)
			return fmt.Errorf("all LUNs are used, cannot attach volume (%s, %s) to instance %q (%v)", diskName, diskURI, instanceid, err)
		}

		// vm is managed by availability set.
		return ss.availabilitySet.AttachDisk(isManagedDisk, diskName, diskURI, nodeName, lun, cachingMode)
	}

	// 4. Node is managed by vmss, attach with scaleSet.AttachDisk.
	instanceid, err := c.cloud.InstanceID(context.TODO(), nodeName)
	if err != nil {
		glog.Warningf("failed to get azure instance id (%v)", err)
		return fmt.Errorf("failed to get azure instance id for node %q (%v)", nodeName, err)
	}

	diskOpMutex.LockKey(instanceid)
	defer diskOpMutex.UnlockKey(instanceid)

	lun, err := c.GetNextDiskLun(nodeName)
	if err != nil {
		glog.Warningf("no LUN available for instance %q (%v)", nodeName, err)
		return fmt.Errorf("all LUNs are used, cannot attach volume (%s, %s) to instance %q (%v)", diskName, diskURI, instanceid, err)
	}

	glog.V(2).Infof("Trying to attach volume %q lun %d to node %q.", diskURI, lun, nodeName)
	return ss.AttachDisk(isManagedDisk, diskName, diskURI, nodeName, lun, cachingMode)
}

// DetachDisk detaches a disk from host. The vhd can be identified by diskName or diskURI.
func (c *controllerCommon) DetachDisk(diskName, diskURI string, nodeName types.NodeName) error {
	// 1. vmType is standard, detach with availabilitySet.DetachDiskByName.
	if c.cloud.VMType == vmTypeStandard {
		instanceid, err := c.cloud.InstanceID(context.TODO(), nodeName)
		if err != nil {
			glog.Warningf("failed to get azure instance id (%v)", err)
			return fmt.Errorf("failed to get azure instance id for node %q (%v)", nodeName, err)
		}

		glog.V(2).Infof("detach %v from node %q", diskURI, nodeName)

		// make the lock here as small as possible
		diskOpMutex.LockKey(instanceid)
		_, err = c.cloud.vmSet.DetachDisk(diskName, diskURI, nodeName)
		diskOpMutex.UnlockKey(instanceid)
		return err
	}

	// 2. vmType is Virtual Machine Scale Set (vmss), convert vmSet to scaleSet.
	ss, ok := c.cloud.vmSet.(*scaleSet)
	if !ok {
		return fmt.Errorf("error of converting vmSet (%q) to scaleSet with vmType %q", c.cloud.vmSet, c.cloud.VMType)
	}

	// 3. If the node is managed by availability set, then detach with availabilitySet.DetachDiskByName.
	managedByAS, err := ss.isNodeManagedByAvailabilitySet(mapNodeNameToVMName(nodeName))
	if err != nil {
		return err
	}
	if managedByAS {
		// vm is managed by availability set.
		instanceid, err := c.cloud.InstanceID(context.TODO(), nodeName)
		if err != nil {
			glog.Warningf("failed to get azure instance id (%v)", err)
			return fmt.Errorf("failed to get azure instance id for node %q (%v)", nodeName, err)
		}

		glog.V(2).Infof("detach %v from node %q", diskURI, nodeName)

		// make the lock here as small as possible
		diskOpMutex.LockKey(instanceid)
		_, err = ss.availabilitySet.DetachDisk(diskName, diskURI, nodeName)
		diskOpMutex.UnlockKey(instanceid)
		return err
	}

	// 4. Node is managed by vmss, detach with scaleSet.DetachDiskByName.
	instanceid, err := c.cloud.InstanceID(context.TODO(), nodeName)
	if err != nil {
		glog.Warningf("failed to get azure instance id (%v)", err)
		return fmt.Errorf("failed to get azure instance id for node %q (%v)", nodeName, err)
	}

	glog.V(2).Infof("detach %v from node %q", diskURI, nodeName)

	// make the lock here as small as possible
	diskOpMutex.LockKey(instanceid)
	resp, err := ss.DetachDisk(diskName, diskURI, nodeName)
	diskOpMutex.UnlockKey(instanceid)

	if c.cloud.CloudProviderBackoff && shouldRetryHTTPRequest(resp, err) {
		glog.V(2).Infof("azureDisk - update backing off: detach disk(%s, %s), err: %v", diskName, diskURI, err)
		retryErr := kwait.ExponentialBackoff(c.cloud.requestBackoff(), func() (bool, error) {
			diskOpMutex.LockKey(instanceid)
			resp, err := ss.DetachDisk(diskName, diskURI, nodeName)
			diskOpMutex.UnlockKey(instanceid)
			return processHTTPRetryResponse(resp, err)
		})
		if retryErr != nil {
			err = retryErr
			glog.V(2).Infof("azureDisk - update abort backoff: detach disk(%s, %s), err: %v", diskName, diskURI, err)
		}
	}
	if err != nil {
		glog.Errorf("azureDisk - detach disk(%s, %s) failed, err: %v", diskName, diskURI, err)
	} else {
		glog.V(2).Infof("azureDisk - detach disk(%s, %s) succeeded", diskName, diskURI)
	}

	return err
}

// GetDiskLun finds the lun on the host that the vhd is attached to, given a vhd's diskName and diskURI.
func (c *controllerCommon) GetDiskLun(diskName, diskURI string, nodeName types.NodeName) (int32, error) {
	// 1. vmType is standard, get with availabilitySet.GetDiskLun.
	if c.cloud.VMType == vmTypeStandard {
		return c.cloud.vmSet.GetDiskLun(diskName, diskURI, nodeName)
	}

	// 2. vmType is Virtual Machine Scale Set (vmss), convert vmSet to scaleSet.
	ss, ok := c.cloud.vmSet.(*scaleSet)
	if !ok {
		return -1, fmt.Errorf("error of converting vmSet (%q) to scaleSet with vmType %q", c.cloud.vmSet, c.cloud.VMType)
	}

	// 3. If the node is managed by availability set, then get with availabilitySet.GetDiskLun.
	managedByAS, err := ss.isNodeManagedByAvailabilitySet(mapNodeNameToVMName(nodeName))
	if err != nil {
		return -1, err
	}
	if managedByAS {
		// vm is managed by availability set.
		return ss.availabilitySet.GetDiskLun(diskName, diskURI, nodeName)
	}

	// 4. Node is managed by vmss, get with scaleSet.GetDiskLun.
	return ss.GetDiskLun(diskName, diskURI, nodeName)
}

// GetNextDiskLun searches all vhd attachment on the host and find unused lun. Return -1 if all luns are used.
func (c *controllerCommon) GetNextDiskLun(nodeName types.NodeName) (int32, error) {
	// 1. vmType is standard, get with availabilitySet.GetNextDiskLun.
	if c.cloud.VMType == vmTypeStandard {
		return c.cloud.vmSet.GetNextDiskLun(nodeName)
	}

	// 2. vmType is Virtual Machine Scale Set (vmss), convert vmSet to scaleSet.
	ss, ok := c.cloud.vmSet.(*scaleSet)
	if !ok {
		return -1, fmt.Errorf("error of converting vmSet (%q) to scaleSet with vmType %q", c.cloud.vmSet, c.cloud.VMType)
	}

	// 3. If the node is managed by availability set, then get with availabilitySet.GetNextDiskLun.
	managedByAS, err := ss.isNodeManagedByAvailabilitySet(mapNodeNameToVMName(nodeName))
	if err != nil {
		return -1, err
	}
	if managedByAS {
		// vm is managed by availability set.
		return ss.availabilitySet.GetNextDiskLun(nodeName)
	}

	// 4. Node is managed by vmss, get with scaleSet.GetNextDiskLun.
	return ss.GetNextDiskLun(nodeName)
}

// DisksAreAttached checks if a list of volumes are attached to the node with the specified NodeName.
func (c *controllerCommon) DisksAreAttached(diskNames []string, nodeName types.NodeName) (map[string]bool, error) {
	// 1. vmType is standard, check with availabilitySet.DisksAreAttached.
	if c.cloud.VMType == vmTypeStandard {
		return c.cloud.vmSet.DisksAreAttached(diskNames, nodeName)
	}

	// 2. vmType is Virtual Machine Scale Set (vmss), convert vmSet to scaleSet.
	ss, ok := c.cloud.vmSet.(*scaleSet)
	if !ok {
		return nil, fmt.Errorf("error of converting vmSet (%q) to scaleSet with vmType %q", c.cloud.vmSet, c.cloud.VMType)
	}

	// 3. If the node is managed by availability set, then check with availabilitySet.DisksAreAttached.
	managedByAS, err := ss.isNodeManagedByAvailabilitySet(mapNodeNameToVMName(nodeName))
	if err != nil {
		return nil, err
	}
	if managedByAS {
		// vm is managed by availability set.
		return ss.availabilitySet.DisksAreAttached(diskNames, nodeName)
	}

	// 4. Node is managed by vmss, check with scaleSet.DisksAreAttached.
	return ss.DisksAreAttached(diskNames, nodeName)
}
