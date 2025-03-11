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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
	Scheme        *runtime.Scheme
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

	dfi, err := getDragonflyInstance(ctx, req.NamespacedName, r, log)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "could not get Dragonfly instance")
		}
		return ctrl.Result{}, nil
	}

	log.Info("Reconciling Dragonfly object")
	if result, err := dfi.ensureDragonflyResources(ctx); !result.IsZero() || err != nil {
		return result, err
	}

	if dfi.df.Status.IsRollingUpdate {
		// This is a Rollout
		log.Info("Rolling out new version")
		statefulSet, err := dfi.getStatefulSet(ctx)
		if err != nil {
			log.Error(err, "could not get statefulset")
			return ctrl.Result{Requeue: true}, err
		}

		pods, err := dfi.getPods(ctx)
		if err != nil {
			log.Error(err, "could not list pods")
			return ctrl.Result{Requeue: true}, err
		}

		if len(pods.Items) != int(*statefulSet.Spec.Replicas) {
			log.Info("Waiting for all replicas to be ready")
			return ctrl.Result{Requeue: true}, nil
		}

		// filter pods to master and replicas
		master, replicas := classifyPods(pods)
		if err != nil {
			log.Error(err, "could not get master and replicas")
			return ctrl.Result{Requeue: true}, nil
		}

		// We want to update the replicas first then the master
		// We want to have at most one updated replica in full sync phase at a time
		// if not, requeue
		if result, err := dfi.checkUpdatedReplicas(ctx, statefulSet, replicas); !result.IsZero() || err != nil {
			return result, err
		}

		// if we are here it means that all latest replicas are in stable sync
		// delete older version replicas
		if result, err := dfi.deleteOldReplicas(ctx, statefulSet, replicas); !result.IsZero() || err != nil {
			return result, err
		}

		var latestReplica *corev1.Pod
		if len(replicas) > 0 {
			latestReplica, err = dfi.getLatestReplica(ctx, statefulSet)
			if err != nil {
				log.Error(err, "could not get latest replica")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
		}

		// If we are here it means that all replicas
		// are on latest version
		if !isPodOnLatestVersion(master, statefulSet) {
			// Update master now
			if latestReplica != nil {
				log.Info("Running REPLTAKEOVER on replica", "pod", master.Name)
				if err := dfi.replTakeover(ctx, latestReplica); err != nil {
					log.Error(err, "could not update master")
					return ctrl.Result{RequeueAfter: 5 * time.Second}, err
				}
			}
			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", fmt.Sprintf("Shutting down master %s", master.Name))

			// delete the old master, so that it gets recreated with the new version
			log.Info("deleting master", "pod", master.Name)
			if err := r.Delete(ctx, master); err != nil {
				log.Error(err, "could not delete pod")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
		}

		// If we are here all are on latest version
		r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Completed")

		// update status
		dfi.df.Status.IsRollingUpdate = false
		if err := r.Status().Update(ctx, dfi.df); err != nil {
			log.Error(err, "could not update the Dragonfly object")
			return ctrl.Result{Requeue: true}, err
		}

		return ctrl.Result{}, nil
	} else {
		log.Info("Checking if pod spec has changed")
		isRollingUpdate, err := dfi.isRollingUpdate(ctx)
		if err != nil {
			log.Error(err, "could not check if it is rolling update")
			return ctrl.Result{Requeue: true}, nil
		}
		// perform a rollout only if the pod spec has changed
		// Check if the pod spec has changed
		if isRollingUpdate {
			log.Info("Pod spec has changed, performing a rollout")
			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Starting a rollout")

			// Start rollout and update status
			// update status so that we can track progress
			dfi.df.Status.IsRollingUpdate = true
			if err := r.Status().Update(ctx, dfi.df); err != nil {
				log.Error(err, "could not update the Dragonfly object")
				return ctrl.Result{Requeue: true}, nil
			}

			r.EventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Performing a rollout")
		}

	}
	return ctrl.Result{Requeue: true}, nil
}

func (r *DragonflyReconciler) GetClient() client.Client { return r.Client }

func (r *DragonflyReconciler) GetEventRecorder() record.EventRecorder { return r.EventRecorder }

func (r *DragonflyReconciler) GetScheme() *runtime.Scheme { return r.Scheme }

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
