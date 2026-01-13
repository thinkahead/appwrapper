/*
Copyright 2024 IBM Corporation.

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

package appwrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	kresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	awv1beta2 "github.com/project-codeflare/appwrapper/api/v1beta2"
	utilmaps "github.com/project-codeflare/appwrapper/internal/util"
	"github.com/project-codeflare/appwrapper/pkg/utils"
)

func parseComponent(raw []byte, expectedNamespace string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	if _, _, err := unstructured.UnstructuredJSONScheme.Decode(raw, nil, obj); err != nil {
		return nil, err
	}
	namespace := obj.GetNamespace()
	if namespace == "" {
		obj.SetNamespace(expectedNamespace)
	} else if namespace != expectedNamespace {
		// Should not happen, namespace equality checked by validateAppWrapperInvariants
		return nil, fmt.Errorf("component namespace \"%s\" is different from appwrapper namespace \"%s\"", namespace, expectedNamespace)
	}
	return obj, nil
}

func hasResourceRequest(spec map[string]interface{}, resource string) bool {
	usesResource := func(container map[string]interface{}) bool {
		_, ok := container["resources"]
		if !ok {
			return false
		}
		resources, ok := container["resources"].(map[string]interface{})
		if !ok {
			return false
		}
		for _, key := range []string{"limits", "requests"} {
			if _, ok := resources[key]; ok {
				if list, ok := resources[key].(map[string]interface{}); ok {
					if _, ok := list[resource]; ok {
						switch quantity := list[resource].(type) {
						case int:
							if quantity > 0 {
								return true
							}
						case int32:
							if quantity > 0 {
								return true
							}
						case int64:
							if quantity > 0 {
								return true
							}
						case string:
							kq, err := kresource.ParseQuantity(quantity)
							if err == nil && !kq.IsZero() {
								return true
							}
						}
					}
				}
			}
		}
		return false
	}

	for _, key := range []string{"containers", "initContainers"} {
		if containers, ok := spec[key]; ok {
			if carray, ok := containers.([]interface{}); ok {
				for _, containerI := range carray {
					container, ok := containerI.(map[string]interface{})
					if ok && usesResource(container) {
						return true
					}
				}
			}
		}
	}

	return false
}

func addNodeSelectorsToAffinity(spec map[string]interface{}, exprsToAdd []v1.NodeSelectorRequirement, required bool, weight int32) error {
	if _, ok := spec["affinity"]; !ok {
		spec["affinity"] = map[string]interface{}{}
	}
	affinity, ok := spec["affinity"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("spec.affinity is not a map")
	}
	if _, ok := affinity["nodeAffinity"]; !ok {
		affinity["nodeAffinity"] = map[string]interface{}{}
	}
	nodeAffinity, ok := affinity["nodeAffinity"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("spec.affinity.nodeAffinity is not a map")
	}
	if required {
		if _, ok := nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"]; !ok {
			nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"] = map[string]interface{}{}
		}
		nodeSelector, ok := nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution is not a map")
		}
		if _, ok := nodeSelector["nodeSelectorTerms"]; !ok {
			nodeSelector["nodeSelectorTerms"] = []interface{}{map[string]interface{}{}}
		}
		existingTerms, ok := nodeSelector["nodeSelectorTerms"].([]interface{})
		if !ok {
			return fmt.Errorf("spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms is not an array")
		}
		for idx, term := range existingTerms {
			selTerm, ok := term.(map[string]interface{})
			if !ok {
				return fmt.Errorf("spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[%v] is not an map", idx)
			}
			if _, ok := selTerm["matchExpressions"]; !ok {
				selTerm["matchExpressions"] = []interface{}{}
			}
			matchExpressions, ok := selTerm["matchExpressions"].([]interface{})
			if !ok {
				return fmt.Errorf("spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[%v].matchExpressions is not an map", idx)
			}
			for _, expr := range exprsToAdd {
				bytes, err := json.Marshal(expr)
				if err != nil {
					return fmt.Errorf("marshalling selectorTerm %v: %w", expr, err)
				}
				var obj interface{}
				if err = json.Unmarshal(bytes, &obj); err != nil {
					return fmt.Errorf("unmarshalling selectorTerm %v: %w", expr, err)
				}
				matchExpressions = append(matchExpressions, obj)
			}
			selTerm["matchExpressions"] = matchExpressions
		}
	} else {
		if _, ok := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"]; !ok {
			nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"] = []interface{}{}
		}
		terms, ok := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"].([]interface{})
		if !ok {
			return fmt.Errorf("spec.affinity.nodeAffinity.preferredDuringSchedulingIgnoredDuringExecution is not an array")
		}
		bytes, err := json.Marshal(v1.PreferredSchedulingTerm{Weight: weight, Preference: v1.NodeSelectorTerm{MatchExpressions: exprsToAdd}})
		if err != nil {
			return fmt.Errorf("marshalling selectorTerms %v: %w", exprsToAdd, err)
		}
		var obj interface{}
		if err = json.Unmarshal(bytes, &obj); err != nil {
			return fmt.Errorf("unmarshalling selectorTerms %v: %w", exprsToAdd, err)
		}
		terms = append(terms, obj)
		nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"] = terms
	}

	return nil
}

//gocyclo:ignore
func (r *AppWrapperReconciler) createComponent(ctx context.Context, aw *awv1beta2.AppWrapper, componentIdx int) (error, bool) {
	component := aw.Spec.Components[componentIdx]
	componentStatus := aw.Status.ComponentStatus[componentIdx]
	toMap := func(x interface{}) map[string]string {
		if x == nil {
			return nil
		} else {
			if sm, ok := x.(map[string]string); ok {
				return sm
			} else if im, ok := x.(map[string]interface{}); ok {
				sm := make(map[string]string, len(im))
				for k, v := range im {
					str, ok := v.(string)
					if ok {
						sm[k] = str
					} else {
						sm[k] = fmt.Sprint(v)
					}
				}
				return sm
			} else {
				return nil
			}
		}
	}

	obj, err := parseComponent(component.Template.Raw, aw.Namespace)
	if err != nil {
		return err, true
	}
	awLabels := map[string]string{awv1beta2.AppWrapperLabel: aw.Name}
	obj.SetLabels(utilmaps.MergeKeepFirst(obj.GetLabels(), awLabels))

	for podSetsIdx, podSet := range componentStatus.PodSets {
		toInject := &awv1beta2.AppWrapperPodSetInfo{}
		if podSetsIdx < len(component.PodSetInfos) {
			toInject = &component.PodSetInfos[podSetsIdx]
		}

		p, err := utils.GetRawTemplate(obj.UnstructuredContent(), podSet.Path)
		if err != nil {
			return err, true // Should not happen, path validity is enforced by validateAppWrapperInvariants
		}
		if md, ok := p["metadata"]; !ok || md == nil {
			p["metadata"] = make(map[string]interface{})
		}
		metadata := p["metadata"].(map[string]interface{})
		spec := p["spec"].(map[string]interface{}) // Must exist, enforced by validateAppWrapperInvariants

		// Annotations
		if len(toInject.Annotations) > 0 {
			existing := toMap(metadata["annotations"])
			if err := utilmaps.HaveConflict(existing, toInject.Annotations); err != nil {
				return fmt.Errorf("conflict updating annotations: %w", err), true
			}
			metadata["annotations"] = utilmaps.MergeKeepFirst(existing, toInject.Annotations)
		}

		// Labels
		mergedLabels := utilmaps.MergeKeepFirst(toInject.Labels, awLabels)
		existing := toMap(metadata["labels"])
		if err := utilmaps.HaveConflict(existing, mergedLabels); err != nil {
			return fmt.Errorf("conflict updating labels: %w", err), true
		}
		metadata["labels"] = utilmaps.MergeKeepFirst(existing, mergedLabels)

		// NodeSelectors
		if len(toInject.NodeSelector) > 0 {
			existing := toMap(spec["nodeSelector"])
			if err := utilmaps.HaveConflict(existing, toInject.NodeSelector); err != nil {
				return fmt.Errorf("conflict updating nodeSelector: %w", err), true
			}
			spec["nodeSelector"] = utilmaps.MergeKeepFirst(existing, toInject.NodeSelector)
		}

		// Tolerations
		if len(toInject.Tolerations) > 0 {
			if _, ok := spec["tolerations"]; !ok {
				spec["tolerations"] = []interface{}{}
			}
			tolerations := spec["tolerations"].([]interface{})
			for _, addition := range toInject.Tolerations {
				tolerations = append(tolerations, addition)
			}
			spec["tolerations"] = tolerations
		}

		// SchedulingGates
		if len(toInject.SchedulingGates) > 0 {
			if _, ok := spec["schedulingGates"]; !ok {
				spec["schedulingGates"] = []interface{}{}
			}
			schedulingGates := spec["schedulingGates"].([]interface{})
			for _, addition := range toInject.SchedulingGates {
				duplicate := false
				for _, existing := range schedulingGates {
					if imap, ok := existing.(map[string]interface{}); ok {
						if iName, ok := imap["name"]; ok {
							if sName, ok := iName.(string); ok && sName == addition.Name {
								duplicate = true
								break
							}
						}
					}
				}
				if !duplicate {
					schedulingGates = append(schedulingGates, map[string]interface{}{"name": addition.Name})
				}
			}
			spec["schedulingGates"] = schedulingGates
		}

		// Scheduler Name
		if r.Config.SchedulerName != "" {
			if existing, _ := spec["schedulerName"].(string); existing == "" {
				spec["schedulerName"] = r.Config.SchedulerName
			}
		}

		if r.Config.Autopilot != nil && r.Config.Autopilot.InjectAntiAffinities {
			toAddRequired := map[string][]string{}
			toAddPreferred := map[string][]string{}
			for resource, taints := range r.Config.Autopilot.ResourceTaints {
				if hasResourceRequest(spec, resource) {
					for _, taint := range taints {
						if taint.Effect == v1.TaintEffectNoExecute || taint.Effect == v1.TaintEffectNoSchedule {
							toAddRequired[taint.Key] = append(toAddRequired[taint.Key], taint.Value)
						} else if taint.Effect == v1.TaintEffectPreferNoSchedule {
							toAddPreferred[taint.Key] = append(toAddPreferred[taint.Key], taint.Value)
						}
					}
				}
			}
			if len(toAddRequired) > 0 {
				matchExpressions := []v1.NodeSelectorRequirement{}
				for k, v := range toAddRequired {
					matchExpressions = append(matchExpressions, v1.NodeSelectorRequirement{Operator: v1.NodeSelectorOpNotIn, Key: k, Values: v})
				}
				if err := addNodeSelectorsToAffinity(spec, matchExpressions, true, 0); err != nil {
					log.FromContext(ctx).Error(err, "failed to inject Autopilot affinities")
				}
			}
			if len(toAddPreferred) > 0 {
				matchExpressions := []v1.NodeSelectorRequirement{}
				for k, v := range toAddPreferred {
					matchExpressions = append(matchExpressions, v1.NodeSelectorRequirement{Operator: v1.NodeSelectorOpNotIn, Key: k, Values: v})
				}
				weight := ptr.Deref(r.Config.Autopilot.PreferNoScheduleWeight, 1)
				if err := addNodeSelectorsToAffinity(spec, matchExpressions, false, weight); err != nil {
					log.FromContext(ctx).Error(err, "failed to inject Autopilot affinities")
				}
			}
		}
	}

	if err := controllerutil.SetControllerReference(aw, obj, r.Scheme); err != nil {
		return err, true
	}

	orig := copyForStatusPatch(aw)
	if meta.FindStatusCondition(aw.Status.ComponentStatus[componentIdx].Conditions, string(awv1beta2.ResourcesDeployed)) == nil {
		aw.Status.ComponentStatus[componentIdx].Name = obj.GetName()
		aw.Status.ComponentStatus[componentIdx].Kind = obj.GetKind()
		aw.Status.ComponentStatus[componentIdx].APIVersion = obj.GetAPIVersion()
		meta.SetStatusCondition(&aw.Status.ComponentStatus[componentIdx].Conditions, metav1.Condition{
			Type:   string(awv1beta2.ResourcesDeployed),
			Status: metav1.ConditionUnknown,
			Reason: "ComponentCreationInitiated",
		})
		if err := r.Status().Patch(ctx, aw, client.MergeFrom(orig)); err != nil {
			return err, false
		}
	}

	if err := r.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// obj is not updated if Create returns an error; Get required for accurate information
			if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
				return err, false
			}
			ctrlRef := metav1.GetControllerOf(obj)
			if ctrlRef == nil || ctrlRef.Name != aw.Name {
				return fmt.Errorf("resource %v exists, but is not controlled by appwrapper", obj.GetName()), true
			}
			// fall through.  This is not actually an error. The object already exists and the correct appwrapper owns it.
		} else {
			// resource not actually created; patch status to reflect that
			orig := copyForStatusPatch(aw)
			meta.SetStatusCondition(&aw.Status.ComponentStatus[componentIdx].Conditions, metav1.Condition{
				Type:   string(awv1beta2.ResourcesDeployed),
				Status: metav1.ConditionFalse,
				Reason: "ComponentCreationErrored",
			})
			if patchErr := r.Status().Patch(ctx, aw, client.MergeFrom(orig)); patchErr != nil {
				// ugh.  Patch failed, so retry the create so we can get to a consistient state
				return patchErr, false
			}
			// return actual error
			return err, meta.IsNoMatchError(err) || apierrors.IsInvalid(err) // fatal
		}
	}

	orig = copyForStatusPatch(aw)
	aw.Status.ComponentStatus[componentIdx].Name = obj.GetName() // Update name to support usage of GenerateName
	meta.SetStatusCondition(&aw.Status.ComponentStatus[componentIdx].Conditions, metav1.Condition{
		Type:   string(awv1beta2.ResourcesDeployed),
		Status: metav1.ConditionTrue,
		Reason: "ComponentCreatedSuccessfully",
	})
	if err := r.Status().Patch(ctx, aw, client.MergeFrom(orig)); err != nil {
		return err, false
	}

	return nil, false
}

// createComponents incrementally patches aw.Status -- MUST NOT CARRY STATUS PATCHES ACROSS INVOCATIONS
func (r *AppWrapperReconciler) createComponents(ctx context.Context, aw *awv1beta2.AppWrapper) (error, bool) {
	for componentIdx := range aw.Spec.Components {
		if !meta.IsStatusConditionTrue(aw.Status.ComponentStatus[componentIdx].Conditions, string(awv1beta2.ResourcesDeployed)) {
			if err, fatal := r.createComponent(ctx, aw, componentIdx); err != nil {
				return err, fatal
			}
		}
	}
	return nil, false
}

func (r *AppWrapperReconciler) deleteComponents(ctx context.Context, aw *awv1beta2.AppWrapper) bool {
	deleteIfPresent := func(idx int, opts ...client.DeleteOption) bool {
		cs := &aw.Status.ComponentStatus[idx]
		rd := meta.FindStatusCondition(cs.Conditions, string(awv1beta2.ResourcesDeployed))
		if rd == nil || rd.Status == metav1.ConditionFalse || (rd.Status == metav1.ConditionUnknown && cs.Name == "") {
			return false // not present
		}
		obj := &metav1.PartialObjectMetadata{
			TypeMeta:   metav1.TypeMeta{Kind: cs.Kind, APIVersion: cs.APIVersion},
			ObjectMeta: metav1.ObjectMeta{Name: cs.Name, Namespace: aw.Namespace},
		}
		if err := r.Delete(ctx, obj, opts...); err != nil {
			if apierrors.IsNotFound(err) {
				// Has already been undeployed; update componentStatus and return not present
				meta.SetStatusCondition(&cs.Conditions, metav1.Condition{
					Type:   string(awv1beta2.ResourcesDeployed),
					Status: metav1.ConditionFalse,
					Reason: "CompononetDeleted",
				})
				return false
			} else {
				log.FromContext(ctx).Error(err, "Deletion error")
				return true // unexpected error ==> still present
			}
		}
		return true // still present
	}

	meta.SetStatusCondition(&aw.Status.Conditions, metav1.Condition{
		Type:   string(awv1beta2.DeletingResources),
		Status: metav1.ConditionTrue,
		Reason: "DeletionInitiated",
	})

	componentsRemaining := false
	for componentIdx := range aw.Spec.Components {
		componentsRemaining = deleteIfPresent(componentIdx, client.PropagationPolicy(metav1.DeletePropagationBackground)) || componentsRemaining
	}

	deletionGracePeriod := r.forcefulDeletionGraceDuration(ctx, aw)
	whenInitiated := meta.FindStatusCondition(aw.Status.Conditions, string(awv1beta2.DeletingResources)).LastTransitionTime
	gracePeriodExpired := time.Now().After(whenInitiated.Time.Add(deletionGracePeriod))

	if componentsRemaining && !gracePeriodExpired {
		// Resources left and deadline hasn't expired, just requeue the deletion
		return false
	}

	pods := &v1.PodList{Items: []v1.Pod{}}
	if err := r.List(ctx, pods,
		client.UnsafeDisableDeepCopy,
		client.InNamespace(aw.Namespace),
		client.MatchingLabels{awv1beta2.AppWrapperLabel: aw.Name}); err != nil {
		log.FromContext(ctx).Error(err, "Pod list error")
	}

	if !componentsRemaining && len(pods.Items) == 0 {
		// no resources or pods left; deletion is complete
		clearCondition(aw, awv1beta2.DeletingResources, "DeletionComplete", "")
		return true
	}

	if gracePeriodExpired {
		if len(pods.Items) > 0 {
			// force deletion of pods first
			for _, pod := range pods.Items {
				if err := r.Delete(ctx, &pod, client.GracePeriodSeconds(0)); err != nil {
					log.FromContext(ctx).Error(err, "Forceful pod deletion error")
				}
			}
		} else {
			// force deletion of wrapped resources once pods are gone
			for componentIdx := range aw.Spec.Components {
				_ = deleteIfPresent(componentIdx, client.GracePeriodSeconds(0))
			}
		}
	}

	// requeue deletion
	return false
}

// checkResourceAvailability verifies that at least one node has sufficient resources
// to satisfy each pod's resource requirements (especially GPUs).
// This prevents transitioning to Running when resources are fragmented across nodes.
func (r *AppWrapperReconciler) checkResourceAvailability(ctx context.Context, aw *awv1beta2.AppWrapper) (bool, error) {
	// Extract all pod resource requirements from the AppWrapper
	podRequests, err := r.extractPodResourceRequests(ctx, aw)
	if err != nil {
		return false, err
	}

	// If no pods request GPUs or other extended resources, skip the check
	if len(podRequests) == 0 {
		return true, nil
	}

	// Get all nodes in the cluster
	nodes := &v1.NodeList{}
	if err := r.List(ctx, nodes); err != nil {
		return false, fmt.Errorf("failed to list nodes: %w", err)
	}

	// Get all pods to calculate allocated resources per node
	pods := &v1.PodList{}
	if err := r.List(ctx, pods); err != nil {
		return false, fmt.Errorf("failed to list pods: %w", err)
	}

	// Build a map of allocated resources per node
	allocatedPerNode := make(map[string]v1.ResourceList)
	for _, pod := range pods.Items {
		// Only count running and pending pods that haven't finished
		if pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodPending {
			nodeName := pod.Spec.NodeName
			if nodeName == "" {
				// For pending pods, we can't determine node allocation yet
				// We'll be conservative and not count them
				continue
			}
			if allocatedPerNode[nodeName] == nil {
				allocatedPerNode[nodeName] = make(v1.ResourceList)
			}
			for _, container := range pod.Spec.Containers {
				for resourceName, quantity := range container.Resources.Requests {
					existing := allocatedPerNode[nodeName][resourceName]
					existing.Add(quantity)
					allocatedPerNode[nodeName][resourceName] = existing
				}
			}
			for _, container := range pod.Spec.InitContainers {
				for resourceName, quantity := range container.Resources.Requests {
					existing := allocatedPerNode[nodeName][resourceName]
					existing.Add(quantity)
					allocatedPerNode[nodeName][resourceName] = existing
				}
			}
		}
	}

	// Get noSchedule resources to subtract from available capacity
	noScheduleNodesMutex.RLock()
	noScheduleResourcesCopy := make(map[string]v1.ResourceList)
	for nodeName, resources := range noScheduleNodes {
		noScheduleResourcesCopy[nodeName] = resources.DeepCopy()
	}
	noScheduleNodesMutex.RUnlock()

	// Check each pod's requirements against available nodes
	for podIdx, podRequest := range podRequests {
		foundSuitableNode := false

		for _, node := range nodes.Items {
			// Skip unschedulable nodes
			if node.Spec.Unschedulable {
				continue
			}

			// Skip nodes that are not ready
			nodeReady := false
			for _, condition := range node.Status.Conditions {
				if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
					nodeReady = true
					break
				}
			}
			if !nodeReady {
				continue
			}

			// Check if this node can satisfy the pod's requirements
			canSchedule := true
			for resourceName, requestedQuantity := range podRequest {
				// Get node capacity
				capacity := node.Status.Allocatable[resourceName]

				// Subtract already allocated resources
				allocated := allocatedPerNode[node.Name][resourceName]
				available := capacity.DeepCopy()
				available.Sub(allocated)

				// Subtract noSchedule resources (e.g., unhealthy GPUs)
				if noScheduleRes, exists := noScheduleResourcesCopy[node.Name]; exists {
					if noScheduleQuantity, hasResource := noScheduleRes[resourceName]; hasResource {
						available.Sub(noScheduleQuantity)
					}
				}

				// Check if available resources are sufficient
				if available.Cmp(requestedQuantity) < 0 {
					canSchedule = false
					break
				}
			}

			if canSchedule {
				foundSuitableNode = true
				log.FromContext(ctx).V(1).Info("Found suitable node for pod",
					"podIndex", podIdx,
					"nodeName", node.Name,
					"podRequests", podRequest)
				break
			}
		}

		if !foundSuitableNode {
			log.FromContext(ctx).Info("No suitable node found with sufficient resources",
				"podIndex", podIdx,
				"podRequests", podRequest,
				"appwrapper", aw.Name)
			return false, nil
		}
	}

	return true, nil
}

// extractPodResourceRequests extracts resource requests from all pods in the AppWrapper
// Returns a slice of ResourceLists, one per pod that requests extended resources
func (r *AppWrapperReconciler) extractPodResourceRequests(ctx context.Context, aw *awv1beta2.AppWrapper) ([]v1.ResourceList, error) {
	var podRequests []v1.ResourceList

	// Iterate through all components
	for componentIdx, component := range aw.Spec.Components {
		componentStatus := aw.Status.ComponentStatus[componentIdx]

		obj, err := parseComponent(component.Template.Raw, aw.Namespace)
		if err != nil {
			return nil, fmt.Errorf("failed to parse component %d: %w", componentIdx, err)
		}

		// Extract pod specs from the component
		for _, podSet := range componentStatus.PodSets {
			podTemplateSpec, err := utils.GetRawTemplate(obj.UnstructuredContent(), podSet.Path)
			if err != nil {
				return nil, fmt.Errorf("failed to get pod template at path %s: %w", podSet.Path, err)
			}

			spec, ok := podTemplateSpec["spec"].(map[string]interface{})
			if !ok {
				continue
			}

			// Extract resource requests from containers
			requests := make(v1.ResourceList)
			hasExtendedResources := false

			extractFromContainers := func(containerList []interface{}) {
				for _, containerI := range containerList {
					container, ok := containerI.(map[string]interface{})
					if !ok {
						continue
					}

					resources, ok := container["resources"].(map[string]interface{})
					if !ok {
						continue
					}

					requestsMap, ok := resources["requests"].(map[string]interface{})
					if !ok {
						continue
					}

					for resourceName, quantityI := range requestsMap {
						// Parse quantity
						var quantity kresource.Quantity
						switch v := quantityI.(type) {
						case string:
							parsed, err := kresource.ParseQuantity(v)
							if err != nil {
								continue
							}
							quantity = parsed
						case int:
							quantity = *kresource.NewQuantity(int64(v), kresource.DecimalSI)
						case int32:
							quantity = *kresource.NewQuantity(int64(v), kresource.DecimalSI)
						case int64:
							quantity = *kresource.NewQuantity(v, kresource.DecimalSI)
						default:
							continue
						}

						resourceNameTyped := v1.ResourceName(resourceName)

						// Check if this is an extended resource (not CPU or memory)
						// Extended resources need per-node checking
						if resourceNameTyped != v1.ResourceCPU && resourceNameTyped != v1.ResourceMemory {
							hasExtendedResources = true
						}

						// Accumulate requests (for multiple containers in same pod)
						existing := requests[resourceNameTyped]
						existing.Add(quantity)
						requests[resourceNameTyped] = existing
					}
				}
			}

			// Extract from regular containers
			if containers, ok := spec["containers"].([]interface{}); ok {
				extractFromContainers(containers)
			}

			// Extract from init containers (they also need resources)
			if initContainers, ok := spec["initContainers"].([]interface{}); ok {
				extractFromContainers(initContainers)
			}

			// Only add to list if this pod requests extended resources (like GPUs)
			if hasExtendedResources && len(requests) > 0 {
				// Get the number of replicas for this podset
				replicas := int32(1)
				if podSet.Replicas != nil {
					replicas = *podSet.Replicas
				}

				// Add one entry per replica
				for i := int32(0); i < replicas; i++ {
					podRequests = append(podRequests, requests.DeepCopy())
				}
			}
		}
	}

	return podRequests, nil
}
