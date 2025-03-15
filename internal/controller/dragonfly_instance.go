/*
Copyright 2023 DragonflyDB authors.

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

package controller

import (
	"context"
	"errors"
	"fmt"
	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	"github.com/redis/go-redis/v9"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"net"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
	"time"
)

type (
	ObjectMetaPatch struct {
		metav1.ObjectMeta `json:"metadata,omitempty"`
	}
	StatusPatch struct {
		Status dfv1alpha1.DragonflyStatus `json:"status,omitempty"`
	}
)

func (dfi *DragonflyInstance) deleteMasterPodRoleLabel(ctx context.Context, pods *corev1.PodList) error {
	for _, pod := range pods.Items {
		if isMaster(pod) {
			patch := []byte(fmt.Sprintf(`[
				{
					"op": "remove",
					"path": "/metadata/labels/%s"
				}
			]`, resources.Role))

			dfi.log.Info("deleting master role label", "pod", pod.Name)
			if err := dfi.client.Patch(ctx, &pod, client.RawPatch(types.JSONPatchType, patch)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (dfi *DragonflyInstance) configureReplication(ctx context.Context, pods *corev1.PodList) error {
	dfi.log.Info("configuring replication")

	master, ok := getReadyPod(pods)
	if ok {
		if err := dfi.replicaOfNoOne(ctx, master); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("failed to find a healthy pod to configure as master")
	}

	for _, pod := range pods.Items {
		if pod.Name != master.Name && isPodReady(pod) {
			if err := dfi.configureReplica(ctx, &pod, master.Status.PodIP); err != nil {
				dfi.log.Error(err, "could not mark replica from db. retrying")
				return err
			}
		}
	}

	if err := dfi.patchStatusPhase(ctx, PhaseReady); err != nil {
		return err
	}

	return nil
}

func (dfi *DragonflyInstance) patchStatusPhase(ctx context.Context, phase string) error {
	dfi.log.Info("Updating status", "phase", phase)

	patchObject := StatusPatch{
		Status: dfv1alpha1.DragonflyStatus{
			Phase: phase,
		},
	}

	patch, err := createPatch(patchObject)
	if err != nil {
		return err
	}

	if err := dfi.client.Status().Patch(ctx, dfi.df, patch); err != nil {
		return err
	}

	return nil
}

// configureReplica marks the given pod as a replica by finding
// a master for that instance
func (dfi *DragonflyInstance) configureReplica(ctx context.Context, pod *corev1.Pod, masterIp string) error {
	dfi.log.Info("configuring pod as replica", "pod", pod.Name, "ip", pod.Status.PodIP)

	if err := dfi.replicaOf(ctx, pod, masterIp); err != nil {
		return err
	}

	return nil
}

// checkReplicaRole checks if the given pod is a replica and if it is
// connected to the right master
func (dfi *DragonflyInstance) checkReplicaRole(ctx context.Context, pod *corev1.Pod, masterIp string) (bool, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(resources.DragonflyAdminPort)),
	})
	defer redisClient.Close()

	resp, err := redisClient.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}

	var redisRole string
	for _, line := range strings.Split(resp, "\n") {
		if strings.Contains(line, "role") {
			redisRole = strings.Trim(strings.Split(line, ":")[1], "\r")
		}
	}

	if redisRole != resources.Replica {
		return false, nil
	}

	var redisMasterIp string
	// check if it is connected to the right master
	for _, line := range strings.Split(resp, "\n") {
		if strings.Contains(line, "master_host") {
			redisMasterIp = strings.Trim(strings.Split(line, ":")[1], "\r")
		}
	}

	if masterIp != redisMasterIp && masterIp != pod.Labels[resources.MasterIp] {
		return false, nil
	}

	return true, nil
}

// checkAndConfigureReplication checks if all the pods are assigned to
// the correct role and if not, configures them accordingly
func (dfi *DragonflyInstance) checkAndConfigureReplication(ctx context.Context, pods *corev1.PodList, sts *appsv1.StatefulSet) (ctrl.Result, error) {
	dfi.log.Info("checking if all pods are configured correctly")

	if !isAllPodsReady(pods, sts) {
		dfi.log.Info("waiting for all pods to be ready")
		return ctrl.Result{Requeue: true}, nil
	}

	master, replicas, err := classifyPods(pods)
	if err != nil {
		switch {
		case errors.Is(err, ErrNoMaster) || errors.Is(err, ErrIncorrectMasters):
			dfi.log.Info("incorrect number of masters. reconfiguring replication", "error", err)
			if err = dfi.configureReplication(ctx, pods); err != nil {
				dfi.log.Error(err, "failed to configure replication")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		case errors.Is(err, ErrIncorrectReplicas):
			dfi.log.Info("incorrect number of replicas", "master", master.Name, "replicas", len(replicas), "pods", len(pods.Items), "error", err)
			for _, pod := range pods.Items {
				if !roleExists(pod) && isPodReady(pod) {
					if err := dfi.configureReplica(ctx, &pod, master.Status.PodIP); err != nil {
						dfi.log.Error(err, "failed to mark replica from db. retrying")
						return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
					}

				}
			}
		default:
			fmt.Println("unexpected error:", err)
		}
	}

	for _, pod := range pods.Items {
		if isReplica(pod) {
			ok, err := dfi.checkReplicaRole(ctx, &pod, master.Status.PodIP)
			if err != nil {
				dfi.log.Error(err, "failed to check replica role", "pod", pod.Name)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			// configuring to the right master
			if !ok {
				dfi.log.Info("configuring pod as replica to the right master", "pod", pod.Name)
				if err := dfi.configureReplica(ctx, &pod, master.Status.PodIP); err != nil {
					dfi.log.Error(err, "could not mark replica from db. retrying")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}
		}
	}

	dfi.log.Info("all pods are configured correctly", "dfi", dfi.df.Name)
	return ctrl.Result{}, nil
}

// getStatefulSet gets the statefulset object for the dragonfly instance
func (dfi *DragonflyInstance) getStatefulSet(ctx context.Context) (*appsv1.StatefulSet, error) {
	dfi.log.Info("getting statefulset")
	var sts appsv1.StatefulSet
	if err := dfi.client.Get(ctx, client.ObjectKey{Namespace: dfi.df.Namespace, Name: dfi.df.Name}, &sts); err != nil {
		dfi.log.Error(err, "failed to get statefulset", "namespace", dfi.df.Namespace, "name", dfi.df.Name)
		return nil, err
	}
	return &sts, nil
}

func (dfi *DragonflyInstance) getPods(ctx context.Context) (*corev1.PodList, error) {
	dfi.log.Info("getting all pods relevant to the instance")
	var pods corev1.PodList
	if err := dfi.client.List(ctx, &pods, client.InNamespace(dfi.df.Namespace), client.MatchingLabels{
		resources.DragonflyNameLabelKey:    dfi.df.Name,
		resources.KubernetesPartOfLabelKey: "dragonfly",
	},
	); err != nil {
		dfi.log.Error(err, "failed to list pods relevant to the instance")
		return nil, err
	}

	return &pods, nil
}

// replicaOf configures the pod as a replica
// to the given master instance
func (dfi *DragonflyInstance) replicaOf(ctx context.Context, pod *corev1.Pod, masterIp string) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(resources.DragonflyAdminPort)),
	})
	defer redisClient.Close()

	dfi.log.Info("trying to invoke SLAVE OF command", "pod", pod.Name, "master", masterIp, "addr", redisClient.Options().Addr)
	resp, err := redisClient.SlaveOf(ctx, masterIp, fmt.Sprint(resources.DragonflyAdminPort)).Result()
	if err != nil {
		return fmt.Errorf("error running SLAVE OF command: %s", err)
	}

	if resp != "OK" {
		return fmt.Errorf("response of `SLAVE OF` on replica is not OK: %s", resp)
	}
	//TODO: remove
	if dfi.client == nil {
		return fmt.Errorf("kubernetes client is not initialized")
	}
	if pod == nil {
		return fmt.Errorf("pod is nil")
	}
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	dfi.log.Info("marking pod role as replica", "pod", pod.Name)

	patchObject := ObjectMetaPatch{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				resources.Role:     resources.Replica,
				resources.MasterIp: masterIp,
			},
		},
	}

	patch, err := createPatch(patchObject)
	if err != nil {
		return err
	}

	if err := dfi.client.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("could not patch replica label: %w", err)
	}

	return nil
}

// replicaOfNoOne configures the pod as a master
// along while updating other pods to be replicas
func (dfi *DragonflyInstance) replicaOfNoOne(ctx context.Context, pod *corev1.Pod) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(resources.DragonflyAdminPort)),
	})
	defer redisClient.Close()

	dfi.log.Info("Running SLAVE OF NO ONE command", "pod", pod.Name, "addr", redisClient.Options().Addr)
	resp, err := redisClient.SlaveOf(ctx, "NO", "ONE").Result()
	if err != nil {
		return fmt.Errorf("error running SLAVE OF NO ONE command: %w", err)
	}

	if resp != "OK" {
		return fmt.Errorf("response of `SLAVE OF NO ONE` on master is not OK: %s", resp)
	}

	dfi.log.Info("marking pod role as master", "pod", pod.Name)

	patchObject := ObjectMetaPatch{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				resources.Role: resources.Master,
			},
		},
	}

	patch, err := createPatch(patchObject)
	if err != nil {
		return err
	}

	if err := dfi.client.Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("could not patch role label: %w", err)
	}

	return nil
}

// ensureDragonflyResources makes sure the dragonfly resources exist and are up to date.
func (dfi *DragonflyInstance) ensureDragonflyResources(ctx context.Context) (ctrl.Result, error) {
	dfi.log.Info("ensuring dragonfly resources")
	dragonflyResources, err := resources.GenerateDragonflyResources(dfi.df)
	if err != nil {
		dfi.log.Error(err, "failed to generate dragonfly resources")
		return ctrl.Result{}, nil
	}

	for _, resource := range dragonflyResources {
		resourceInfo := map[string]string{
			"Kind":      getGVK(resource, dfi.scheme).Kind,
			"Namespace": resource.GetNamespace(),
			"Name":      resource.GetName(),
		}
		existingResource := resource.DeepCopyObject().(client.Object)

		if err = dfi.client.Get(ctx, client.ObjectKey{
			Namespace: dfi.df.Namespace,
			Name:      resource.GetName()},
			existingResource,
		); err != nil {
			if apierrors.IsNotFound(err) {
				dfi.log.Info("creating resource", "resource", resourceInfo)
				if err = dfi.client.Create(ctx, resource); err != nil {
					dfi.log.Error(err, "failed to create resource", "resource", resourceInfo)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
				continue
			}
			dfi.log.Error(err, "failed to get resource", "resource", resourceInfo)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		dfi.log.Info("updating resource", "resource", resourceInfo)

		patch, err := createPatch(resource)
		if err != nil {
			dfi.log.Error(err, "failed to create patch")
			return ctrl.Result{}, nil
		}

		if err := dfi.client.Patch(ctx, existingResource, patch); err != nil {
			dfi.log.Error(err, "failed to patch resource", "resource", resourceInfo)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if dfi.df.Status.Phase == "" {
		dfi.log.Info("updating dragonfly status to ResourcesCreated")
		if err := dfi.patchStatusPhase(ctx, PhaseResourcesCreated); err != nil {
			dfi.log.Error(err, "failed to update the dragonfly object")

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Created resources")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	dfi.log.Info("dragonfly resources are up to date")

	return ctrl.Result{}, nil
}

// isRollingUpdate checks if the given Dragonfly object is in a rolling update state.
func (dfi *DragonflyInstance) isRollingUpdate(sts *appsv1.StatefulSet, pods *corev1.PodList) bool {
	if sts.Status.UpdatedReplicas != sts.Status.Replicas {
		for _, pod := range pods.Items {
			if !isPodOnLatestVersion(&pod, sts) {
				return true
			}
		}
	}

	return false
}

// checkUpdatedReplicas checks if the updated replicas are in a stable state
func (dfi *DragonflyInstance) verifyUpdatedReplicas(ctx context.Context, sts *appsv1.StatefulSet, replicas []*corev1.Pod) (ctrl.Result, error) {
	fullSyncedReplicas := 0
	for _, replica := range replicas {
		if isPodOnLatestVersion(replica, sts) {
			dfi.log.Info("New Replica found. Checking if replica had a full sync", "pod", replica.Name)

			ok, err := isReplicaStable(ctx, replica)
			if err != nil {
				dfi.log.Error(err, "could not check if pod is in stable state")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			if !ok {
				dfi.log.Info("Not all new replicas are in stable status yet", "pod", replica.Name, "reason", err)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			dfi.log.Info("Replica is in stable state", "pod", replica.Name)
			fullSyncedReplicas++
		}
	}

	dfi.log.Info("Replicas where verified", "synced_replicas", fullSyncedReplicas, "replicas", len(replicas))

	return ctrl.Result{}, nil
}

func (dfi *DragonflyInstance) deleteCrashLoopBackOff(ctx context.Context, pods *corev1.PodList, sts *appsv1.StatefulSet) (ctrl.Result, error) {
	for _, pod := range pods.Items {
		if !isPodOnLatestVersion(&pod, sts) && isPodCrashLoopBackOff(pod) {
			dfi.log.Info("deleting pod in CrashLoopBackOff...", "pod", pod.Name)
			if err := dfi.client.Delete(ctx, &pod); err != nil {
				dfi.log.Error(err, "failed not delete pod")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// deleteOldReplicas deletes the old replicas
func (dfi *DragonflyInstance) deleteOldReplicas(ctx context.Context, sts *appsv1.StatefulSet, replicas []*corev1.Pod) (ctrl.Result, error) {
	for _, replica := range replicas {
		if !isPodOnLatestVersion(replica, sts) {
			// delete the replica
			dfi.log.Info("deleting replica", "pod", replica.Name)
			dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Deleting replica")
			if err := dfi.client.Delete(ctx, replica); err != nil {
				dfi.log.Error(err, "failed not delete pod")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// getLatestReplica returns a replica pod which is on the latest version
// of the given statefulset
func (dfi *DragonflyInstance) getLatestReplica(sts *appsv1.StatefulSet, replicas []*corev1.Pod) (*corev1.Pod, error) {
	// Iterate over the replicas and find a replica which is on the latest version
	for _, replica := range replicas {
		if isPodOnLatestVersion(replica, sts) {
			return replica, nil
		}
	}

	return nil, fmt.Errorf("no replica pod found on latest version")
}

// replTakeover runs the replTakeOver on the given replica pod
func (dfi *DragonflyInstance) replTakeover(ctx context.Context, newMaster *corev1.Pod) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: net.JoinHostPort(newMaster.Status.PodIP, strconv.Itoa(resources.DragonflyAdminPort)),
	})
	defer redisClient.Close()

	resp, err := redisClient.Do(ctx, "repltakeover", "10000").Result()
	if err != nil {
		return fmt.Errorf("error running REPLTAKEOVER command: %w", err)
	}

	if resp != "OK" {
		return fmt.Errorf("response of `REPLTAKEOVER` on replica is not OK: %s", resp)
	}

	// update the label on the pod
	patchObject := ObjectMetaPatch{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				resources.Role: resources.Master,
			},
		},
	}

	patch, err := createPatch(patchObject)
	if err != nil {
		return err
	}

	if err := dfi.client.Patch(ctx, newMaster, patch); err != nil {
		return fmt.Errorf("failed to patch replica label: %w", err)
	}

	return nil
}
