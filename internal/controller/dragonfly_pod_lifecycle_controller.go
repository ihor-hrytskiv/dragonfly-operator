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
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"time"
)

type DfPodLifeCycleReconciler struct {
	Reconciler
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// This reconcile events focuses on configuring the given pods either as a `master`
// or `replica` as they go through their lifecycle. This also focus on the failing
// over to replica's part to make sure one `master` is always available.
func (r *DfPodLifeCycleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	log.Info("received", "pod", req.NamespacedName)
	var pod corev1.Pod
	if err := r.Client.Get(ctx, req.NamespacedName, &pod, &client.GetOptions{}); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	dfName, ok := pod.Labels[resources.DragonflyNameLabelKey]
	if !ok {
		log.Info("failed to get Dragonfly name from pod labels")
		return ctrl.Result{}, nil
	}

	dfi, err := r.getDragonflyInstance(ctx, types.NamespacedName{
		Name:      dfName,
		Namespace: pod.Namespace,
	}, log)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pods, err := dfi.getPods(ctx)
	if err != nil {
		log.Error(err, "could not list pods")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// New pod with No resources.Role
	if isPodReady(pod) && !roleExists(pod) {
		log.Info("pod does not have a role label", "phase", dfi.df.Status.Phase)
		master, err := getMaster(pods)
		if err != nil {

			if errors.Is(err, ErrNoMaster) {
				log.Info("the master pod does not exist. Configuring replication...")
			}

			if errors.Is(err, ErrIncorrectMasters) {
				//remove master pod label if it exists
				// This is important as the pod termination could take a while in
				// the deleted case causing unnecessary master reconcilation as 2 masters
				// could exist at the same time.
				if err = dfi.deleteMasterPodRoleLabel(ctx, pods); err != nil {
					log.Error(err, "Failed to delete master role label.")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
			}

			if err := dfi.configureReplication(ctx, pods); err != nil {
				log.Error(err, "failed to configure replication.")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Replication", "Updated master instance")

		} else {
			log.Info("the master exists. Configuring the replica...", "namespace", pod.Namespace, "pod", pod.Name, "ip", pod.Status.PodIP)

			if err := dfi.configureReplica(ctx, &pod, master.Status.PodIP); err != nil {
				log.Error(err, "failed to mark replica from db. retrying")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Replication", "Configured a new replica")
		}
	}

	if isPodMarkedForDeletion(pod) {
		// pod deletion event
		// configure replication if its a master pod
		// do nothing if its a replica pod
		log.Info("pod is being deleted", "namespace", pod.Namespace, "pod", pod.Name)
		// Check if there is an active master
		if isMaster(pod) {
			log.Info("master is being removed")
			if dfi.df.Status.Phase == PhaseRollingUpdate {
				log.Info("rolling update in progress. nothing to do")
				return ctrl.Result{}, nil
			}

			log.Info("master is being deleted. configuring replication")
			if err := dfi.configureReplication(ctx, pods); err != nil {
				log.Info("failed to reconfigure replication after master deletion", "error", err)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Replication", "Updated master instance")
		} else if isReplica(pod) {
			log.Info("replica is being deleted. nothing to do")
		}
	}

	if isPodReady(pod) && roleExists(pod) {
		if dfi.df.Status.Phase == PhaseRollingUpdate {
			log.Info("rolling update in progress. nothing to do")
			return ctrl.Result{}, nil
		}

		// is something wrong? check if all pods have a matching role and revamp accordingly
		log.Info("non-deletion event for a pod with an existing role. checking if something is wrong", "namespace", pod.Namespace, "pod", pod.Name, "role", pod.Labels[resources.Role])

		statefulSet, err := dfi.getStatefulSet(ctx)
		if err != nil {
			log.Error(err, "failed to get statefulset")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		if result, err := dfi.checkAndConfigureReplication(ctx, pods, statefulSet); !result.IsZero() || err != nil {
			return result, err
		}

		r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Replication", "Checked and configured replication")
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DfPodLifeCycleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithEventFilter(
			predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					return e.ObjectNew.GetLabels()[resources.KubernetesAppNameLabelKey] == "dragonfly"
				},
				CreateFunc: func(e event.CreateEvent) bool {
					return e.Object.GetLabels()[resources.KubernetesAppNameLabelKey] == "dragonfly"
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return e.Object.GetLabels()[resources.KubernetesAppNameLabelKey] == "dragonfly"
				},
			}).
		Named("DragonflyPodLifecycle").
		For(&corev1.Pod{}).
		Complete(r)
}
