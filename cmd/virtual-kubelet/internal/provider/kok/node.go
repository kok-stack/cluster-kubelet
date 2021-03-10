package kok

import (
	"context"
	"github.com/kok-stack/cluster-kubelet/log"
	"github.com/kok-stack/cluster-kubelet/trace"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1listers "k8s.io/client-go/listers/core/v1"
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

func sub(total corev1.ResourceList, item corev1.ResourceList) {
	for name, val := range item {
		r, ok := total[name]
		if !ok {
			continue
		}
		r.Sub(val)
		total[name] = r
	}
}

type NodeEventHandler struct {
	ctx            context.Context
	p              *Provider
	node           *v1.Node
	notifyNodeFunc func(*v1.Node)
	updateNode     chan struct{}
	downNodeLister corev1listers.NodeLister
	downPodLister  corev1listers.PodLister
}

func (n *NodeEventHandler) OnAdd(obj interface{}) {
	node := obj.(*v1.Node)
	add(n.node.Status.Capacity, node.Status.Capacity)
	add(n.node.Status.Allocatable, node.Status.Allocatable)

	n.updateNode <- struct{}{}
}

func (n *NodeEventHandler) OnDelete(obj interface{}) {
	node := obj.(*v1.Node)

	sub(n.node.Status.Capacity, node.Status.Capacity)
	sub(n.node.Status.Allocatable, node.Status.Allocatable)

	n.updateNode <- struct{}{}
}

func (n *NodeEventHandler) OnUpdate(oldObj, newObj interface{}) {
	n.OnAdd(newObj)
	n.OnDelete(oldObj)
}

func (n *NodeEventHandler) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(n.updateNode)
				return
			case <-n.updateNode:
				log.G(ctx).Debug("开始更新node status的 Allocatable/Capacity")

				n.notifyNodeFunc(n.node)
				log.G(ctx).Debug("完成node status的 Allocatable/Capacity 更新")
			}
		}
	}()
}

func (n *NodeEventHandler) configureNode(ctx context.Context, node *v1.Node) {
	ctx, span := trace.StartSpan(ctx, "ConfigureNode")
	defer span.End()

	node.Status.NodeInfo.KubeletVersion = n.p.config.Version
	node.Status.Addresses = []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: n.p.config.InternalIP}}
	node.Status.Conditions = nodeConditions()
	node.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: n.p.config.DaemonPort,
		},
	}
	node.Status.NodeInfo.OSImage = n.p.config.Version
	node.Status.NodeInfo.KernelVersion = n.p.config.Version
	node.Status.NodeInfo.OperatingSystem = "linux"
	node.Status.NodeInfo.Architecture = "amd64"
	node.ObjectMeta.Labels[v1.LabelArchStable] = "amd64"
	node.ObjectMeta.Labels[v1.LabelOSStable] = "linux"
	node.ObjectMeta.Labels["alpha.service-controller.kubernetes.io/exclude-balancer"] = "true"
	node.ObjectMeta.Labels["node.kubernetes.io/exclude-from-external-load-balancers"] = "true"

	n.node = node
	n.initResourceList(ctx)
}

func (n *NodeEventHandler) initResourceList(ctx context.Context) {
	nodes, err := n.downNodeLister.List(labels.Nothing())
	if err != nil {
		log.G(ctx).Warnf("获取down集群Node出错", err)
		return
	}

	allocates := make([]v1.ResourceList, len(nodes))
	capacitys := make([]v1.ResourceList, len(nodes))
	for i, n := range nodes {
		allocates[i] = n.Status.Allocatable
		capacitys[i] = n.Status.Capacity
	}
	n.node.Status.Allocatable = totalResourceList(allocates)
	n.node.Status.Capacity = totalResourceList(allocates)
	list, err := n.downPodLister.List(labels.Nothing())
	if err != nil {
		log.G(ctx).Warnf("获取down集群Pods出错", err)
		return
	}
	for _, pod := range list {
		for _, c := range pod.Spec.InitContainers {
			sub(n.node.Status.Allocatable, c.Resources.Requests)
		}
		for _, c := range pod.Spec.Containers {
			sub(n.node.Status.Allocatable, c.Resources.Requests)
		}
	}
	log.G(ctx).Debug("初始化node ResourceList完成")
}

func (n *NodeEventHandler) OnPodAdd(pod *v1.Pod) {
	for _, c := range pod.Spec.InitContainers {
		add(n.node.Status.Allocatable, c.Resources.Requests)
	}
	for _, c := range pod.Spec.Containers {
		add(n.node.Status.Allocatable, c.Resources.Requests)
	}
	n.updateNode <- struct{}{}
}

func (n *NodeEventHandler) OnPodDelete(pod *v1.Pod) {
	for _, c := range pod.Spec.InitContainers {
		sub(n.node.Status.Allocatable, c.Resources.Requests)
	}
	for _, c := range pod.Spec.Containers {
		sub(n.node.Status.Allocatable, c.Resources.Requests)
	}
	n.updateNode <- struct{}{}
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
			Message:            "virtual-kubelet is posting ready status",
		},
		{
			Type:               "MemoryPressure",
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "virtual-kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "virtual-kubelet has no disk pressure",
		},
		{
			Type:               "PIDPressure",
			Status:             corev1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientPID",
			Message:            "virtual-kubelet has sufficient PID available",
		},
	}
}

func (p *Provider) Ping(ctx context.Context) error {
	version, err := p.downClientSet.Discovery().ServerVersion()
	if err != nil {
		log.G(ctx).Warnf("获取down 集群版本错误", err)
		return err
	}
	log.G(ctx).Debugf("down 集群版本:", version.String())
	return nil
}

func (p *Provider) NotifyNodeStatus(ctx context.Context, f func(*v1.Node)) {
	p.nodeHandler.notifyNodeFunc = f
	p.nodeHandler.start(ctx)
}
