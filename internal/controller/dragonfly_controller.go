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
	"fmt"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DragonflyReconciler reconciles a Dragonfly object
type DragonflyReconciler struct {
	Reconciler
}

//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *DragonflyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	log.Info("reconciling dragonfly object")

	dfi, err := r.getDragonflyInstance(ctx, req.NamespacedName, log)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	dragonflyResources, err := resources.GenerateDragonflyResources(dfi.df)
	if err != nil {
		log.Error(err, "failed to generate dragonfly resources")
		return ctrl.Result{}, nil
	}

	for _, resource := range dragonflyResources {
		dfi.log.Info("reconciling dragonfly resource", "Kind", getGVK(resource, r.Scheme).Kind, "Namespace", resource.GetNamespace(), "Name", resource.GetName())

		if err := dfi.reconcileResource(ctx, resource); err != nil {
			log.Error(err, "failed to reconcile dragonfly resource")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	if dfi.df.Status.Phase == "" {
		if err := dfi.updateStatus(ctx, PhaseResourcesCreated); err != nil {
			dfi.log.Error(err, "failed to update the dragonfly object")

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Created resources")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	var statefulSet appsv1.StatefulSet
	if err := r.Client.Get(ctx, client.ObjectKey{Namespace: dfi.df.Namespace, Name: dfi.df.Name}, &statefulSet); err != nil {
		log.Error(err, "failed to get statefulset")
		return ctrl.Result{}, err
	}

	if dfi.df.Status.IsRollingUpdate {
		// This is a Rollout
		log.Info("Rolling out new version")
		var updatedStatefulset appsv1.StatefulSet
		if err := r.Client.Get(ctx, client.ObjectKey{Namespace: dfi.df.Namespace, Name: dfi.df.Name}, &updatedStatefulset); err != nil {
			log.Error(err, "could not get statefulset")
			return ctrl.Result{Requeue: true}, err
		}

		// get pods of the statefulset
		var pods corev1.PodList
		if err := r.Client.List(ctx, &pods, client.InNamespace(dfi.df.Namespace), client.MatchingLabels(map[string]string{
			resources.DragonflyNameLabelKey:     dfi.df.Name,
			resources.KubernetesAppNameLabelKey: "dragonfly",
		})); err != nil {
			log.Error(err, "could not list pods")
			return ctrl.Result{Requeue: true}, err
		}

		if len(pods.Items) != int(*updatedStatefulset.Spec.Replicas) {
			log.Info("Waiting for all replicas to be ready")
			return ctrl.Result{Requeue: true}, nil
		}

		// filter replicas to master and replicas
		var master corev1.Pod
		replicas := make([]corev1.Pod, 0)
		for _, pod := range pods.Items {
			if _, ok := pod.Labels[resources.Role]; ok {
				if pod.Labels[resources.Role] == resources.Replica {
					replicas = append(replicas, pod)
				} else if pod.Labels[resources.Role] == resources.Master {
					master = pod
				}
			} else {
				log.Info("found pod without label", "pod", pod.Name)
				if isFailedToStart(&pod) {
					// This is a new pod which is trying to be ready, but couldn't start due to misconfig.
					// Delete the pod and create a new one.
					if err := r.Client.Delete(ctx, &pod); err != nil {
						log.Error(err, "could not delete pod")
						return ctrl.Result{RequeueAfter: 5 * time.Second}, err
					}
				}
				// retry after they are ready
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}

		// We want to update the replicas first then the master
		// We want to have at most one updated replica in full sync phase at a time
		// if not, requeue
		fullSyncedUpdatedReplicas := 0
		for _, replica := range replicas {
			// Check only with latest replicas
			onLatestVersion, err := isPodOnLatestVersion(&replica, &updatedStatefulset)
			if err != nil {
				log.Error(err, "could not check if pod is on latest version")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
			if onLatestVersion {
				// check if the replica had a full sync
				log.Info("New Replica found. Checking if replica had a full sync", "pod", replica.Name)
				isStableState, err := isStableState(ctx, &replica)
				if err != nil {
					log.Error(err, "could not check if pod is in stable state")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}

				if !isStableState {
					log.Info("Not all new replicas are in stable status yet", "pod", replica.Name, "reason", err)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
				log.Info("Replica is in stable state", "pod", replica.Name)
				fullSyncedUpdatedReplicas++
			}
		}

		log.Info(fmt.Sprintf("%d/%d replicas are in stable state", fullSyncedUpdatedReplicas, len(replicas)))

		// if we are here it means that all latest replicas are in stable sync
		// delete older version replicas
		for _, replica := range replicas {
			// Check if pod is on latest version
			onLatestVersion, err := isPodOnLatestVersion(&replica, &updatedStatefulset)
			if err != nil {
				log.Error(err, "could not check if pod is on latest version")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}

			if !onLatestVersion {
				// delete the replica
				log.Info("deleting replica", "pod", replica.Name)
				r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Deleting replica")
				if err := r.Client.Delete(ctx, &replica); err != nil {
					log.Error(err, "could not delete pod")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}

				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}

		var latestReplica *corev1.Pod
		var err error
		if len(replicas) > 0 {
			latestReplica, err = getLatestReplica(ctx, r.Client, &updatedStatefulset)
			if err != nil {
				log.Error(err, "could not get latest replica")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
		}

		masterOnLatest, err := isPodOnLatestVersion(&master, &updatedStatefulset)
		if err != nil {
			log.Error(err, "could not check if pod is on latest version")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}

		// If we are here it means that all replicas
		// are on latest version
		if !masterOnLatest {
			// Update master now
			if latestReplica != nil {
				log.Info("Running REPLTAKEOVER on replica", "pod", master.Name)
				if err := replTakeover(ctx, r.Client, latestReplica); err != nil {
					log.Error(err, "could not update master")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
			}
			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", fmt.Sprintf("Shutting down master %s", master.Name))

			// delete the old master, so that it gets recreated with the new version
			log.Info("deleting master", "pod", master.Name)
			if err := r.Client.Delete(ctx, &master); err != nil {
				log.Error(err, "could not delete pod")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
		}

		// If we are here all are on latest version
		r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Completed")

		// update status
		dfi.df.Status.IsRollingUpdate = false
		if err := r.Client.Status().Update(ctx, dfi.df); err != nil {
			log.Error(err, "could not update the Dragonfly object")
			return ctrl.Result{Requeue: true}, err
		}

		return ctrl.Result{}, nil
	} else if statefulSet.Status.UpdatedReplicas != statefulSet.Status.Replicas {
		// perform a rollout only if the pod spec has changed
		// Check if the pod spec has changed
		log.Info("Checking if pod spec has changed", "updatedReplicas", statefulSet.Status.UpdatedReplicas, "currentReplicas", statefulSet.Status.Replicas)
		log.Info("Pod spec has changed, performing a rollout")
		r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Starting a rollout")

		// Start rollout and update status
		// update status so that we can track progress
		dfi.df.Status.IsRollingUpdate = true
		if err := r.Client.Status().Update(ctx, dfi.df); err != nil {
			log.Error(err, "could not update the Dragonfly object")
			return ctrl.Result{Requeue: true}, err
		}

		r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Performing a rollout")
	}
	return ctrl.Result{Requeue: true}, nil
}

func isFailedToStart(pod *corev1.Pod) bool {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if (containerStatus.State.Waiting != nil && isFailureReason(containerStatus.State.Waiting.Reason)) ||
			(containerStatus.State.Terminated != nil && isFailureReason(containerStatus.State.Terminated.Reason)) {
			return true
		}
	}
	return false
}

// isFailureReason checks if the given reason indicates a failure.
func isFailureReason(reason string) bool {
	return reason == "ErrImagePull" ||
		reason == "ImagePullBackOff" ||
		reason == "CrashLoopBackOff" ||
		reason == "RunContainerError"
}

// SetupWithManager sets up the controller with the Manager.
func (r *DragonflyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Listen only to spec changes
		For(&dfv1alpha1.Dragonfly{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.StatefulSet{}, builder.MatchEveryOwner).
		Owns(&corev1.Service{}, builder.MatchEveryOwner).
		Named("Dragonfly").
		Complete(r)
}
