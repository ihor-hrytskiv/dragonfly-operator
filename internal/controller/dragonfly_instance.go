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
	"github.com/go-logr/logr"
	"github.com/redis/go-redis/v9"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"net"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strconv"
	"strings"
	"time"
)

// DragonflyInstance is an abstraction over the `Dragonfly` CRD
// and provides methods to handle replication.
type (
	Reconciler interface {
		GetClient() client.Client
		GetScheme() *runtime.Scheme
		GetEventRecorder() record.EventRecorder
	}
	DragonflyInstance struct {
		// Dragonfly is the relevant Dragonfly CRD that it performs actions over
		df *dfv1alpha1.Dragonfly

		client        client.Client
		scheme        *runtime.Scheme
		eventRecorder record.EventRecorder
		log           logr.Logger
	}
)

func getDragonflyInstance(ctx context.Context, namespacedName types.NamespacedName, reconciler Reconciler, log logr.Logger) (*DragonflyInstance, error) {
	// Retrieve the relevant Dragonfly object
	var df dfv1alpha1.Dragonfly
	c := reconciler.GetClient()
	err := c.Get(ctx, namespacedName, &df)
	if err != nil {
		return nil, err
	}

	return &DragonflyInstance{
		df:            &df,
		client:        c,
		scheme:        reconciler.GetScheme(),
		eventRecorder: reconciler.GetEventRecorder(),
		log:           log,
	}, nil
}

func (dfi *DragonflyInstance) configureReplication(ctx context.Context) error {
	dfi.log.Info("Configuring replication")

	pods, err := dfi.getPods(ctx)
	if err != nil {
		return err
	}

	// remove master pod label if it exists
	// This is important as the pod termination could take a while in
	// the deleted case causing unnecessary master reconcilation as 2 masters
	// could exist at the same time.
	for _, pod := range pods.Items {
		if pod.Labels[resources.Role] == resources.Master {
			delete(pod.Labels, resources.Role)
			if err := dfi.client.Update(ctx, &pod); err != nil {
				return err
			}
		}
	}

	var master string
	var masterIp string
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready && pod.DeletionTimestamp == nil && pod.Status.PodIP != "" {
			master = pod.Name
			masterIp = pod.Status.PodIP
			dfi.log.Info("Marking pod as master", "podName", master, "ip", masterIp)
			if err := dfi.replicaOfNoOne(ctx, &pod); err != nil {
				dfi.log.Error(err, "Failed to mark pod as master", "podName", pod.Name)
				return err
			}
			break
		}
	}

	if master == "" {
		dfi.log.Info("Couldn't find a healthy pod to configure as master")
		return errors.New("couldn't find a healthy pod to configure as master")
	}

	// Mark others as replicas
	markedPods := 0
	for _, pod := range pods.Items {
		// only mark the running non-master pods
		dfi.log.Info("Checking pod", "podName", pod.Name, "ip", pod.Status.PodIP, "status", pod.Status.Phase, "deletiontimestamp", pod.DeletionTimestamp)
		if pod.Name != master && pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil && pod.Status.PodIP != "" {
			dfi.log.Info("Marking pod as replica", "podName", pod.Name, "ip", pod.Status.PodIP, "status", pod.Status.Phase)
			if err := dfi.replicaOf(ctx, &pod, masterIp); err != nil {
				// TODO: Why does this fail every now and then?
				// Should replication be continued if it fails?
				dfi.log.Error(err, "Failed to mark pod as replica", "podName", pod.Name)
				return err
			} else {
				markedPods++
			}
		}
	}

	dfi.log.Info(fmt.Sprintf("Successfully marked %d/%d replicas", markedPods, len(pods.Items)-1))
	if err := dfi.updateStatus(ctx, PhaseReady); err != nil {
		return err
	}

	return nil
}

func (dfi *DragonflyInstance) updateStatus(ctx context.Context, phase string) error {
	// get latest df object first
	if err := dfi.client.Get(ctx, types.NamespacedName{
		Name:      dfi.df.Name,
		Namespace: dfi.df.Namespace,
	}, dfi.df); err != nil {
		return err
	}

	dfi.log.Info("Updating status", "phase", phase)
	dfi.df.Status.Phase = phase
	if err := dfi.client.Status().Update(ctx, dfi.df); err != nil {
		return err
	}

	return nil
}

func (dfi *DragonflyInstance) masterExists(ctx context.Context) (bool, error) {
	dfi.log.Info("checking if a master exists already")
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return false, err
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready && pod.Labels[resources.Role] == resources.Master {
			return true, nil
		}
	}

	return false, nil
}

func (dfi *DragonflyInstance) getMasterIp(ctx context.Context) (string, error) {
	dfi.log.Info("retrieving ip of the master")
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return "", err
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready && pod.Labels[resources.Role] == resources.Master && pod.DeletionTimestamp == nil {
			return pod.Status.PodIP, nil
		}
	}

	return "", errors.New("could not find master")
}

// configureReplica marks the given pod as a replica by finding
// a master for that instance
func (dfi *DragonflyInstance) configureReplica(ctx context.Context, pod *corev1.Pod) error {
	dfi.log.Info("configuring pod as replica", "pod", pod.Name)
	masterIp, err := dfi.getMasterIp(ctx)
	if err != nil {
		return err
	}

	if err := dfi.replicaOf(ctx, pod, masterIp); err != nil {
		return err
	}

	if err := dfi.updateStatus(ctx, PhaseReady); err != nil {
		return err
	}

	return nil
}

// checkReplicaRole checks if the given pod is a replica and if it is
// connected to the right master
func (dfi *DragonflyInstance) checkReplicaRole(ctx context.Context, pod *corev1.Pod, masterIp string) (bool, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", pod.Status.PodIP, resources.DragonflyAdminPort),
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
func (dfi *DragonflyInstance) checkAndConfigureReplication(ctx context.Context) error {
	dfi.log.Info("checking if all pods are configured correctly")
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return err
	}

	// retry if there are pods that are not running
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodRunning {
			dfi.log.Info("not all pods are running. retrying", "pod", pod.Name)
			return nil
		}
	}

	// check for one master and all replicas
	podRoles := make(map[string][]string)
	for _, pod := range pods.Items {
		podRoles[pod.Labels[resources.Role]] = append(podRoles[pod.Labels[resources.Role]], pod.Name)
	}

	if len(podRoles[resources.Master]) != 1 {
		dfi.log.Info("incorrect number of masters. reconfiguring replication", "masters", podRoles[resources.Master])
		if err = dfi.configureReplication(ctx); err != nil {
			return err
		}
	}

	if len(podRoles[resources.Replica]) != len(pods.Items)-1 {
		dfi.log.Info("incorrect number of replicas", "replicas", podRoles[resources.Replica])

		// configure non replica pods as replicas
		for _, pod := range pods.Items {
			if pod.Labels[resources.Role] == "" {
				if pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready && pod.Status.PodIP != "" {
					if err := dfi.configureReplica(ctx, &pod); err != nil {
						return err
					}
				}
			}
		}
	}

	masterIp, err := dfi.getMasterIp(ctx)
	if err != nil {
		return err
	}

	for _, pod := range pods.Items {
		if pod.Labels[resources.Role] == resources.Replica {
			ok, err := dfi.checkReplicaRole(ctx, &pod, masterIp)
			if err != nil {
				return err
			}

			// configuring to the right master
			if !ok {
				dfi.log.Info("configuring pod as replica to the right master", "pod", pod.Name)
				if err := dfi.configureReplica(ctx, &pod); err != nil {
					return err
				}
			}
		}
	}

	dfi.log.Info("all pods are configured correctly", "dfi", dfi.df.Name)
	return nil
}

// getStatefulSet gets the statefulset object for the dragonfly instance
func (dfi *DragonflyInstance) getStatefulSet(ctx context.Context) (*appsv1.StatefulSet, error) {
	var sts appsv1.StatefulSet
	if err := dfi.client.Get(ctx, client.ObjectKey{Namespace: dfi.df.Namespace, Name: dfi.df.Name}, &sts); err != nil {
		return nil, fmt.Errorf("failed to get statefulset %s/%s: %w", dfi.df.Namespace, dfi.df.Name, err)
	}
	return &sts, nil
}

func (dfi *DragonflyInstance) getPods(ctx context.Context) (*corev1.PodList, error) {
	dfi.log.Info("getting all pods relevant to the instance")
	var pods corev1.PodList
	if err := dfi.client.List(ctx, &pods, client.InNamespace(dfi.df.Namespace), client.MatchingLabels{
		"app":                              dfi.df.Name,
		resources.KubernetesPartOfLabelKey: "dragonfly",
	},
	); err != nil {
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

	dfi.log.Info("Trying to invoke SLAVE OF command", "pod", pod.Name, "master", masterIp, "addr", redisClient.Options().Addr)
	resp, err := redisClient.SlaveOf(ctx, masterIp, fmt.Sprint(resources.DragonflyAdminPort)).Result()
	if err != nil {
		return fmt.Errorf("error running SLAVE OF command: %s", err)
	}

	if resp != "OK" {
		return fmt.Errorf("response of `SLAVE OF` on replica is not OK: %s", resp)
	}

	dfi.log.Info("Marking pod role as replica", "pod", pod.Name)
	pod.Labels[resources.Role] = resources.Replica
	pod.Labels[resources.MasterIp] = masterIp
	if err := dfi.client.Update(ctx, pod); err != nil {
		return fmt.Errorf("could not update replica label")
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

	dfi.log.Info("Marking pod role as master", "pod", pod.Name)
	pod.Labels[resources.Role] = resources.Master
	if err := dfi.client.Update(ctx, pod); err != nil {
		return err
	}

	return nil
}

// ensureDragonflyResources makes sure the dragonfly resources exist and are up to date.
func (dfi *DragonflyInstance) ensureDragonflyResources(ctx context.Context) (ctrl.Result, error) {
	dfi.log.Info("Ensuring dragonfly resources")
	dragonflyResources, err := resources.GenerateDragonflyResources(ctx, dfi.df)
	if err != nil {
		dfi.log.Error(err, "failed to generate dragonfly resources")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	for _, resource := range dragonflyResources {
		resourceInfo := fmt.Sprintf("%s/%s/%s", getGVK(resource, dfi.scheme).Kind, resource.GetNamespace(), resource.GetName())
		existingResource := resource.DeepCopyObject().(client.Object)

		if err = dfi.client.Get(ctx, client.ObjectKey{
			Namespace: dfi.df.Namespace,
			Name:      resource.GetName()},
			existingResource,
		); err != nil {
			if apierrors.IsNotFound(err) {
				dfi.log.Info(fmt.Sprintf("Creating resource: %s", resourceInfo))
				if err = dfi.client.Create(ctx, resource); err != nil {
					dfi.log.Error(err, "could not create resource", "resource", resourceInfo)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
				continue
			}
			dfi.log.Error(err, "could not get resource", "resource", resourceInfo)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		dfi.log.Info(fmt.Sprintf("Updating resource: %s", resourceInfo))
		if err = dfi.client.Update(ctx, resource); err != nil {
			dfi.log.Error(err, "could not update resource", "resource", resourceInfo)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if dfi.df.Status.Phase == "" {
		dfi.df.Status.Phase = PhaseResourcesCreated
		if err = dfi.client.Status().Update(ctx, dfi.df); err != nil {
			dfi.log.Error(err, "could not update the dragonfly object")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Created resources")
	}

	return ctrl.Result{}, nil
}

// isRollingUpdate checks if the given Dragonfly object is in a rolling update state.
func (dfi *DragonflyInstance) isRollingUpdate(ctx context.Context) (bool, error) {
	sts, err := dfi.getStatefulSet(ctx)
	if err != nil {
		return false, err
	}
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return false, err
	}

	if sts.Status.UpdatedReplicas != sts.Status.Replicas {
		for _, pod := range pods.Items {
			if !isPodOnLatestVersion(&pod, sts) {
				return true, nil
			}
		}
	}
	return false, nil
}

// checkUpdatedReplicas checks if the updated replicas are in a stable state
func (dfi *DragonflyInstance) checkUpdatedReplicas(ctx context.Context, sts *appsv1.StatefulSet, replicas []*corev1.Pod) (ctrl.Result, error) {
	fullSyncedUpdatedReplicas := 0
	for _, replica := range replicas {
		if isPodOnLatestVersion(replica, sts) {
			dfi.log.Info("New Replica found. Checking if replica had a full sync", "pod", replica.Name)
			isStableState, err := isStableState(ctx, replica)
			if err != nil {
				dfi.log.Error(err, "could not check if pod is in stable state")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			if !isStableState {
				dfi.log.Info("Not all new replicas are in stable status yet", "pod", replica.Name, "reason", err)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
			dfi.log.Info("Replica is in stable state", "pod", replica.Name)
			fullSyncedUpdatedReplicas++
		}
	}

	dfi.log.Info(fmt.Sprintf("%d/%d replicas are in stable state", fullSyncedUpdatedReplicas, len(replicas)))

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
				dfi.log.Error(err, "could not delete pod")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// getLatestReplica returns a replica pod which is on the latest version
// of the given statefulset
func (dfi *DragonflyInstance) getLatestReplica(ctx context.Context, sts *appsv1.StatefulSet) (*corev1.Pod, error) {
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return nil, err
	}

	// Iterate over the pods and find a replica which is on the latest version
	for _, pod := range pods.Items {
		if isPodOnLatestVersion(&pod, sts) && isReplica(&pod) {
			return &pod, nil
		}
	}

	return nil, errors.New("no replica pod found on latest version")
}

// replTakeover runs the replTakeOver on the given replica pod
func (dfi *DragonflyInstance) replTakeover(ctx context.Context, newMaster *corev1.Pod) error {
	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", newMaster.Status.PodIP, resources.DragonflyAdminPort),
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
	newMaster.Labels[resources.Role] = resources.Master
	if err := dfi.client.Update(ctx, newMaster); err != nil {
		return fmt.Errorf("error updating the role label on the pod: %w", err)
	}
	return nil
}
