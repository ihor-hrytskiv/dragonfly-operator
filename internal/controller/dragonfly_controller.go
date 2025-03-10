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
	"k8s.io/apimachinery/pkg/labels"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DragonflyReconciler reconciles a Dragonfly object
type DragonflyReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	EventRecorder record.EventRecorder
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

	df, err := r.getDragonfly(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log.Info("Reconciling Dragonfly object", "Phase", df.Status.Phase, "CreationTimestamp", df.CreationTimestamp, "dfSpec", df.Spec, "dfStatus", df.Status)
	if err := r.ensureDragonflyResources(ctx, df); err != nil {
		log.Error(err, "could not manage dragonfly resources")
		return ctrl.Result{}, err
	}

	if df.Status.IsRollingUpdate {
		// This is a Rollout
		log.Info("Rolling out new version")
		statefulSet, err := r.getStatefulSet(ctx, df)
		if err != nil {
			log.Error(err, "could not get statefulset")
			return ctrl.Result{Requeue: true}, err
		}

		// get pods of the statefulset
		pods, err := r.getPods(ctx, statefulSet)
		if err != nil {
			log.Error(err, "could not list pods")
			return ctrl.Result{Requeue: true}, err
		}

		if len(pods.Items) != int(*statefulSet.Spec.Replicas) {
			log.Info("Waiting for all replicas to be ready")
			return ctrl.Result{Requeue: true}, nil
		}

		// filter replicas to master and replicas
		master, replicas := classifyPods(pods)
		if err != nil {
			log.Error(err, "could not get master and replicas")
			return ctrl.Result{Requeue: true}, err
		}

		// We want to update the replicas first then the master
		// We want to have at most one updated replica in full sync phase at a time
		// if not, requeue
		fullSyncedUpdatedReplicas := 0
		for _, replica := range replicas {
			if isPodOnLatestVersion(replica, statefulSet) {
				// check if the replica is fully synced
				log.Info("New Replica found. Checking if replica is fully synced", "pod", replica.Name)
				isReplicaStable, err := isStableState(ctx, replica)
				if err != nil {
					log.Error(err, "could not check if pod is in stable state")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}

				if !isReplicaStable {
					log.Info("Not all new replicas are in stable status yet", "pod", replica.Name, "reason", err)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
				log.Info("Replica is in stable state", "pod", replica.Name)
				fullSyncedUpdatedReplicas++
			}
		}

		log.Info(fmt.Sprintf("%d/%d replicas are fully synced", fullSyncedUpdatedReplicas, len(replicas)))

		// if we are here it means that all latest replicas are in stable sync
		// delete older version replicas
		for _, replica := range replicas {
			if !isPodOnLatestVersion(replica, statefulSet) {
				// delete the replica
				log.Info("deleting replica", "pod", replica.Name)
				r.EventRecorder.Event(df, corev1.EventTypeNormal, "Rollout", "Deleting replica")
				if err := r.Delete(ctx, replica); err != nil {
					log.Error(err, "could not delete pod")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}

				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}

		// If we are here it means that all replicas
		// are on latest version
		if !isPodOnLatestVersion(master, statefulSet) {
			// Update master now
			if len(replicas) > 0 {
				replica, err := getStableReplica(ctx, replicas)
				if err != nil {
					log.Error(err, "could not get stable replica")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
				log.Info("Running REPLTAKEOVER on replica", "pod", master.Name)
				if err := replTakeover(ctx, r.Client, replica); err != nil {
					log.Error(err, "could not update master")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
			}
			r.EventRecorder.Event(df, corev1.EventTypeNormal, "Rollout", fmt.Sprintf("Shutting down master %s", master.Name))

			// delete the old master, so that it gets recreated with the new version
			log.Info("deleting master", "pod", master.Name)
			if err := r.Delete(ctx, master); err != nil {
				log.Error(err, "could not delete pod")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
		}

		// If we are here all are on latest version
		r.EventRecorder.Event(df, corev1.EventTypeNormal, "Rollout", "Completed")

		// update status
		df.Status.IsRollingUpdate = false
		if err := r.Status().Update(ctx, df); err != nil {
			log.Error(err, "could not update the Dragonfly object")
			return ctrl.Result{Requeue: true}, err
		}

		return ctrl.Result{}, nil
	} else {
		log.Info("Checking if pod spec has changed")
		isRollingUpdate, err := r.isRollingUpdate(ctx, df)
		if err != nil {
			log.Error(err, "could not check if it is rolling update")
			return ctrl.Result{Requeue: true}, nil
		}
		// perform a rollout only if the pod spec has changed
		// Check if the pod spec has changed
		if isRollingUpdate {
			log.Info("Pod spec has changed, performing a rollout")
			r.EventRecorder.Event(df, corev1.EventTypeNormal, "Rollout", "Starting a rollout")

			// Start rollout and update status
			// update status so that we can track progress
			df.Status.IsRollingUpdate = true
			if err := r.Status().Update(ctx, df); err != nil {
				log.Error(err, "could not update the Dragonfly object")
				return ctrl.Result{Requeue: true}, nil
			}

			r.EventRecorder.Event(df, corev1.EventTypeNormal, "Resources", "Performing a rollout")
		}

	}
	return ctrl.Result{Requeue: true}, nil
}

// ensureDragonflyResources makes sure the dragonfly resources exist and are up to date.
func (r *DragonflyReconciler) ensureDragonflyResources(ctx context.Context, df *dfv1alpha1.Dragonfly) error {
	dragonflyResources, err := resources.GenerateDragonflyResources(ctx, df)
	if err != nil {
		return fmt.Errorf("failed to generate dragonfly resources: %w", err)
	}

	for _, resource := range dragonflyResources {
		resourceInfo := fmt.Sprintf("%s/%s/%s", getGVK(resource, r.Scheme).Kind, resource.GetNamespace(), resource.GetName())
		existingResource := resource.DeepCopyObject().(client.Object)

		if err = r.Get(ctx, client.ObjectKey{
			Namespace: df.Namespace,
			Name:      resource.GetName()},
			existingResource,
		); err != nil {
			if errors.IsNotFound(err) {
				if err = r.Create(ctx, resource); err != nil {
					return fmt.Errorf("could not create %s: %w", resourceInfo, err)
				}
				continue
			}
			return fmt.Errorf("could not get %s: %w", resourceInfo, err)
		}
		if err = r.Update(ctx, resource); err != nil {
			return fmt.Errorf("could not update %s: %w", resourceInfo, err)
		}
	}

	if df.Status.Phase == "" {
		df.Status.Phase = PhaseResourcesCreated
		if err = r.Status().Update(ctx, df); err != nil {
			return fmt.Errorf("could not update the Dragonfly object: %w", err)
		}

		r.EventRecorder.Event(df, corev1.EventTypeNormal, "Resources", "Created resources")
	}

	return nil
}

// isRollingUpdate checks if the given Dragonfly object is in a rolling update state.
func (r *DragonflyReconciler) isRollingUpdate(ctx context.Context, df *dfv1alpha1.Dragonfly) (bool, error) {
	sts, err := r.getStatefulSet(ctx, df)
	if err != nil {
		return false, err
	}
	pods, err := r.getPods(ctx, sts)
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

// getDragonfly returns the Dragonfly object with the given NamespacedName
func (r *DragonflyReconciler) getDragonfly(ctx context.Context, key client.ObjectKey) (*dfv1alpha1.Dragonfly, error) {
	df := &dfv1alpha1.Dragonfly{}
	if err := r.Get(ctx, key, df); err != nil {
		return nil, fmt.Errorf("failed to get dragonfly %s/%s: %w", key.Namespace, key.Name, err)
	}
	return df, nil
}

// getStatefulSet returns the StatefulSet of the given Dragonfly object.
func (r *DragonflyReconciler) getStatefulSet(ctx context.Context, df *dfv1alpha1.Dragonfly) (*appsv1.StatefulSet, error) {
	sts := &appsv1.StatefulSet{}
	if err := r.Get(ctx, client.ObjectKey{Namespace: df.Namespace, Name: df.Name}, sts); err != nil {
		return nil, fmt.Errorf("failed to get statefulset %s/%s: %w", df.Namespace, df.Name, err)
	}
	return sts, nil
}

// getPods returns the pods of the given StatefulSet.
func (r *DragonflyReconciler) getPods(ctx context.Context, sts *appsv1.StatefulSet) (*corev1.PodList, error) {
	pods := &corev1.PodList{}
	labelSelector := labels.Set(sts.Spec.Selector.MatchLabels)
	if err := r.List(ctx, pods, &client.ListOptions{
		Namespace:     sts.Namespace,
		LabelSelector: labels.SelectorFromSet(labelSelector),
	}); err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}
	return pods, nil
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
