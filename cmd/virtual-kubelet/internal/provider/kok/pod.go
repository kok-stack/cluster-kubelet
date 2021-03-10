package kok

import (
	"context"
	"github.com/kok-stack/cluster-kubelet/cmd/virtual-kubelet/internal/commands/root"
	"github.com/kok-stack/cluster-kubelet/node/api"
	"github.com/kok-stack/cluster-kubelet/trace"
	v1 "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/remotecommand"
	stats "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"reflect"
	"strings"
	"time"
)

func (p *Provider) GetStatsSummary(ctx context.Context) (*stats.Summary, error) {
	ctx, span := trace.StartSpan(ctx, "ConfigureNode")
	defer span.End()

	var summary stats.Summary

	metrics, err := p.downMetricsClientSet.MetricsV1beta1().PodMetricses(v1.NamespaceAll).List(ctx, v12.ListOptions{})
	if err != nil {
		return nil, err
	}
	var cpuAll, memoryAll uint64
	var t time.Time
	for _, metric := range metrics.Items {
		podStats := convert2PodStats(&metric)
		summary.Pods = append(summary.Pods, *podStats)
		cpuAll += *podStats.CPU.UsageNanoCores
		memoryAll += *podStats.Memory.WorkingSetBytes
		if t.IsZero() {
			t = podStats.StartTime.Time
		}
	}
	summary.Node = stats.NodeStats{
		NodeName:  p.config.NodeName,
		StartTime: metav1.Time{Time: t},
		CPU: &stats.CPUStats{
			Time:           metav1.Time{Time: t},
			UsageNanoCores: &cpuAll,
		},
		Memory: &stats.MemoryStats{
			Time:            metav1.Time{Time: t},
			WorkingSetBytes: &memoryAll,
		},
	}
	return &summary, nil
}

func convert2PodStats(metric *v1beta1.PodMetrics) *stats.PodStats {
	stat := &stats.PodStats{}
	if metric == nil {
		return nil
	}
	stat.PodRef.Namespace = metric.Namespace
	stat.PodRef.Name = metric.Name
	stat.StartTime = metric.Timestamp

	containerStats := stats.ContainerStats{}
	var cpuAll, memoryAll uint64
	for _, c := range metric.Containers {
		containerStats.StartTime = metric.Timestamp
		containerStats.Name = c.Name
		nanoCore := uint64(c.Usage.Cpu().ScaledValue(resource.Nano))
		memory := uint64(c.Usage.Memory().Value())
		containerStats.CPU = &stats.CPUStats{
			Time:           metric.Timestamp,
			UsageNanoCores: &nanoCore,
		}
		containerStats.Memory = &stats.MemoryStats{
			Time:            metric.Timestamp,
			WorkingSetBytes: &memory,
		}
		cpuAll += nanoCore
		memoryAll += memory
		stat.Containers = append(stat.Containers, containerStats)
	}
	stat.CPU = &stats.CPUStats{
		Time:           metric.Timestamp,
		UsageNanoCores: &cpuAll,
	}
	stat.Memory = &stats.MemoryStats{
		Time:            metric.Timestamp,
		WorkingSetBytes: &memoryAll,
	}
	return stat
}

func getConfigMaps(pod *v1.Pod) map[string]interface{} {
	m := make(map[string]interface{})
	for _, volume := range pod.Spec.Volumes {
		if volume.ConfigMap != nil {
			m[volume.ConfigMap.Name] = nil
		}
	}
	for _, c := range pod.Spec.InitContainers {
		for _, envVar := range c.Env {
			if envVar.ValueFrom != nil && envVar.ValueFrom.ConfigMapKeyRef != nil {
				m[envVar.ValueFrom.ConfigMapKeyRef.Name] = nil
			}
		}
		for _, s := range c.EnvFrom {
			if s.ConfigMapRef != nil {
				m[s.ConfigMapRef.Name] = nil
			}
		}
	}
	for _, c := range pod.Spec.Containers {
		for _, envVar := range c.Env {
			if envVar.ValueFrom != nil && envVar.ValueFrom.ConfigMapKeyRef != nil {
				m[envVar.ValueFrom.ConfigMapKeyRef.Name] = nil
			}
		}
		for _, s := range c.EnvFrom {
			if s.ConfigMapRef != nil {
				m[s.ConfigMapRef.Name] = nil
			}
		}
	}
	return m
}

func getSecrets(pod *v1.Pod) map[string]interface{} {
	m := make(map[string]interface{})
	for _, volume := range pod.Spec.Volumes {
		if volume.Secret != nil {
			m[volume.Secret.SecretName] = nil
		}
	}
	for _, c := range pod.Spec.InitContainers {
		for _, envVar := range c.Env {
			if envVar.ValueFrom != nil && envVar.ValueFrom.SecretKeyRef != nil {
				m[envVar.ValueFrom.SecretKeyRef.Name] = nil
			}
		}
		for _, s := range c.EnvFrom {
			if s.SecretRef != nil {
				m[s.SecretRef.Name] = nil
			}
		}
	}
	for _, c := range pod.Spec.Containers {
		for _, envVar := range c.Env {
			if envVar.ValueFrom != nil && envVar.ValueFrom.SecretKeyRef != nil {
				m[envVar.ValueFrom.SecretKeyRef.Name] = nil
			}
		}
		for _, s := range c.EnvFrom {
			if s.SecretRef != nil {
				m[s.SecretRef.Name] = nil
			}
		}
	}
	if pod.Spec.ImagePullSecrets != nil {
		for _, s := range pod.Spec.ImagePullSecrets {
			m[s.Name] = nil
		}
	}
	return m
}

// termSize helps exec termSize
type termSize struct {
	attach api.AttachIO
}

// Next returns the new terminal size after the terminal has been resized. It returns nil when
// monitoring has been stopped.
func (t *termSize) Next() *remotecommand.TerminalSize {
	resize := <-t.attach.Resize()
	return &remotecommand.TerminalSize{
		Height: resize.Height,
		Width:  resize.Width,
	}
}

type PodEventHandler struct {
	ctx          context.Context
	p            *Provider
	notifyFunc   func(*v1.Pod)
	podUpdateCh  chan *v1.Pod
	nodeUpdateCh chan struct{}
	nodeHandler  *NodeEventHandler
}

func (p *PodEventHandler) OnAdd(obj interface{}) {
	downPod := obj.(*v1.Pod)
	upPod, err := p.p.config.ResourceManager.GetPod(downPod.Namespace, downPod.Name)
	if err != nil {
		println(err.Error())
		return
	}
	if upPod == nil {
		return
	}
	upPod = upPod.DeepCopy()
	upPod.Spec = downPod.Spec
	upPod.Status = downPod.Status
	p.podUpdateCh <- upPod
	p.nodeHandler.OnPodAdd(upPod)
}

func (p *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	p.OnAdd(newObj)
	oldPod := oldObj.(*v1.Pod)
	p.nodeHandler.OnPodDelete(oldPod)
}

func (p *PodEventHandler) OnDelete(obj interface{}) {
	downPod := obj.(*v1.Pod)
	err := p.p.config.ResourceManager.DeletePod(p.ctx, downPod.GetNamespace(), downPod.GetName())
	if (err != nil && errors2.IsNotFound(err)) || err == nil {
		return
	} else {
		println(err.Error())
	}
	p.nodeHandler.OnPodDelete(downPod)
}

func (p *PodEventHandler) start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(p.podUpdateCh)
				return
			case t := <-p.podUpdateCh:
				p.notifyFunc(t)
			}
		}
	}()
}

func (p *Provider) syncConfigMap(ctx context.Context, cmName string, namespace string) error {
	upConfigMap, err := p.config.ResourceManager.GetConfigMap(cmName, namespace)
	if err != nil {
		return err
	}
	downCM, err := p.downConfigMapLister.ConfigMaps(namespace).Get(cmName)
	if err != nil {
		if !errors2.IsNotFound(err) {
			return err
		}
		//如果不存在则创建
		trimObjectMeta(&upConfigMap.ObjectMeta)
		_, err = p.downClientSet.CoreV1().ConfigMaps(namespace).Create(ctx, upConfigMap, v12.CreateOptions{})
		if err != nil {
			return err
		}
	} else {
		//存在则需要比对内容并更新
		trimObjectMeta(&upConfigMap.ObjectMeta)
		trimObjectMeta(&downCM.ObjectMeta)
		if reflect.DeepEqual(upConfigMap, downCM) {
			return nil
		}
		_, err = p.downClientSet.CoreV1().ConfigMaps(namespace).Update(ctx, upConfigMap, v12.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	return err
}

const downVirtualKubeletLabel = "virtual-kubelet"
const downVirtualKubeletLabelValue = "true"

func getDownPodVirtualKubeletLabels() string {
	return labels.FormatLabels(map[string]string{
		downVirtualKubeletLabel: downVirtualKubeletLabelValue,
	})
}

func addDownPodVirtualKubeletLabels(pod *v1.Pod) {
	l := pod.Labels
	if l == nil {
		l = make(map[string]string)
	}
	l[downVirtualKubeletLabel] = downVirtualKubeletLabelValue
	pod.Labels = l
}

func (p *Provider) syncSecret(ctx context.Context, secretName string, namespace string) error {
	if isDefaultSecret(secretName) {
		return nil
	}
	upSecret, err := p.config.ResourceManager.GetSecret(secretName, namespace)
	if err != nil {
		return err
	}
	downSecret, err := p.downSecretLister.Secrets(namespace).Get(secretName)
	//如果不存在则创建
	if err != nil {
		if !errors2.IsNotFound(err) {
			return err
		}
		trimObjectMeta(&upSecret.ObjectMeta)
		_, err = p.downClientSet.CoreV1().Secrets(namespace).Create(ctx, upSecret, v12.CreateOptions{})
		if err != nil {
			return err
		}
	} else {
		//存在则对比,更新
		trimObjectMeta(&upSecret.ObjectMeta)
		trimObjectMeta(&downSecret.ObjectMeta)
		if reflect.DeepEqual(upSecret, downSecret) {
			return nil
		}
		_, err = p.downClientSet.CoreV1().Secrets(namespace).Update(ctx, upSecret, v12.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

func isDefaultSecret(secretName string) bool {
	return strings.HasPrefix(secretName, defaultTokenNamePrefix)
}

const defaultTokenNamePrefix = "default-token"

/*
通过此处修改pod
1.在down集群中添加 virtual-kubelet标识
2.删除Meta中的部分信息
3.删除nodeName
4.删除默认的Secret
5.设置默认的status
6.删除nodeSelector中virtual-kubelet标识
7.删除tolerations中的virtual-kubelet标识
*/
func trimPod(pod *v1.Pod, nodeName string) {
	addDownPodVirtualKubeletLabels(pod)
	trimObjectMeta(&pod.ObjectMeta)
	pod.Spec.NodeName = ""

	vols := make([]v1.Volume, 0, len(pod.Spec.Volumes))
	for _, v := range pod.Spec.Volumes {
		if isDefaultSecret(v.Name) {
			continue
		}
		vols = append(vols, v)
	}
	pod.Spec.Containers = trimContainers(pod.Spec.Containers)
	pod.Spec.InitContainers = trimContainers(pod.Spec.InitContainers)
	pod.Spec.Volumes = vols
	pod.Status = v1.PodStatus{}
	trimNodeSelector(pod, nodeName)
	trimTolerations(pod)
}

func trimTolerations(pod *v1.Pod) {
	tolerations := pod.Spec.Tolerations
	for i, item := range tolerations {
		if item.Key != root.DefaultTaintKey {
			continue
		}
		tolerations = append(tolerations[0:i], tolerations[i+1:]...)
	}
	pod.Spec.Tolerations = tolerations
}

func trimNodeSelector(pod *v1.Pod, nodeName string) {
	for s, v := range pod.Spec.NodeSelector {
		if s != root.NodeTypeLabel && s != root.NodeRoleLabel && s != root.NodeHostNameLabel {
			continue
		}
		if v != root.NodeRoleValue && v != root.NodeTypeValue && v != nodeName {
			continue
		}
		delete(pod.Spec.NodeSelector, s)
	}
}

func trimObjectMeta(meta *v12.ObjectMeta) {
	meta.SetUID("")
	meta.SetResourceVersion("")
	meta.SetSelfLink("")
	meta.SetOwnerReferences(nil)
}

func trimContainers(containers []v1.Container) []v1.Container {
	var newContainers []v1.Container

	for _, c := range containers {
		var volMounts []v1.VolumeMount
		for _, v := range c.VolumeMounts {
			if isDefaultSecret(v.Name) {
				continue
			}
			volMounts = append(volMounts, v)
		}
		c.VolumeMounts = volMounts
		newContainers = append(newContainers, c)
	}

	return newContainers
}
