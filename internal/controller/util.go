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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"strings"

	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	"github.com/redis/go-redis/v9"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	PhaseResourcesCreated string = "resources-created"

	PhaseReady string = "ready"
)

// isPodOnLatestVersion returns if the Given pod is on the updatedRevision
// of the given statefulset or not
func isPodOnLatestVersion(pod *corev1.Pod, sts *appsv1.StatefulSet) bool {
	if podRevision, ok := pod.Labels[appsv1.StatefulSetRevisionLabel]; ok && podRevision == sts.Status.UpdateRevision {
		return true
	}

	return false
}

func isStableState(ctx context.Context, pod *corev1.Pod) (bool, error) {
	// wait until pod IP is ready
	if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
		return false, nil
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", pod.Status.PodIP, resources.DragonflyAdminPort),
	})
	defer redisClient.Close()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return false, err
	}

	info, err := redisClient.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}

	if info == "" {
		return false, errors.New("empty info")
	}

	data := map[string]string{}
	for _, line := range strings.Split(info, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.Split(line, ":")
		data[kv[0]] = strings.TrimSuffix(kv[1], "\r")
	}

	if data["master_sync_in_progress"] == "1" {
		return false, nil
	}

	if data["master_link_status"] != "up" {
		return false, nil
	}

	if data["master_last_io_seconds_ago"] == "-1" {
		return false, nil
	}

	return true, nil
}

// getGVK returns the GroupVersionKind of the given object.
func getGVK(obj client.Object, scheme *runtime.Scheme) schema.GroupVersionKind {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return schema.GroupVersionKind{Group: "Unknown", Version: "Unknown", Kind: "Unknown"}
	}
	return gvk
}

// classifyPods classifies the given pods into master and replicas.
func classifyPods(pods *corev1.PodList) (*corev1.Pod, []*corev1.Pod) {
	master := &corev1.Pod{}
	replicas := make([]*corev1.Pod, 0)
	for _, pod := range pods.Items {
		if _, ok := pod.Labels[resources.Role]; ok {
			if isReplica(&pod) {
				replicas = append(replicas, &pod)
			} else if isMaster(&pod) {
				master = &pod
			}
		}
	}
	return master, replicas
}

func isMaster(pod *corev1.Pod) bool {
	if role, ok := pod.Labels[resources.Role]; ok && role == resources.Master {
		return true
	}
	return false
}

func isReplica(pod *corev1.Pod) bool {
	if role, ok := pod.Labels[resources.Role]; ok && role == resources.Replica {
		return true
	}
	return false
}
