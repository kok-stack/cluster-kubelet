package kok

import (
	"context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func totalResourceList(rs []corev1.ResourceList) corev1.ResourceList {
	total := make(map[corev1.ResourceName]resource.Quantity)
	for _, item := range rs {
		add(total, item)
	}
	return total
}

func add(total corev1.ResourceList, item corev1.ResourceList) {
	for name, quantity := range item {
		r, ok := total[name]
		if !ok {
			r = quantity
		} else {
			r.Add(quantity)
		}
		total[name] = r
	}

}

type NodeEventHandler struct {
	ctx context.Context
	p   *Provider
}

//TODO:计算virtual node资源
func (n *NodeEventHandler) OnAdd(obj interface{}) {
	//panic("implement me")
}

func (n *NodeEventHandler) OnDelete(obj interface{}) {
	//panic("implement me")
}

func (n *NodeEventHandler) OnUpdate(oldObj, newObj interface{}) {
	n.OnDelete(oldObj)
	n.OnAdd(newObj)
}

// nodeConditions creates a slice of node conditions representing a
// kubelet in perfect health. These four conditions are the ones which virtual-kubelet
// sets as Unknown when a Ping fails.
func nodeConditions() []corev1.NodeCondition {
	return []corev1.NodeCondition{
		{
			Type:               "Ready",
			Status:             corev1.ConditionTrue,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletReady",
			Message:            "kubelet is posting ready status",
		},
		{
			Type:               "MemoryPressure",
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
		{
			Type:               "PIDPressure",
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientPID",
			Message:            "kubelet has sufficient PID available",
		},
	}
}
