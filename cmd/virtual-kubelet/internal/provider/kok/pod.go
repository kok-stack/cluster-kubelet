package kok

import (
	"context"
	"fmt"
	"github.com/kok-stack/cluster-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/remotecommand"
	"reflect"
	"strings"
)

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

//TODO:计算virtual pod使用的资源,并考虑重启后如何获取当前已使用资源
type PodEventHandler struct {
	ctx context.Context
	p   *Provider
}

func (p *PodEventHandler) OnAdd(obj interface{}) {
	downPod := obj.(*v1.Pod)
	fmt.Println("PodEventHandler.OnAdd==================================")
	upPod, err := p.p.config.ResourceManager.GetPod(downPod.Namespace, downPod.Name)
	if err != nil {
		println(err.Error())
		return
	}
	if upPod == nil {
		return
	}
	upPod = upPod.DeepCopy()
	upPod.Status = downPod.Status
	p.p.notifier(upPod)
}

func (p *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	downPod := newObj.(*v1.Pod)
	upPod, err := p.p.config.ResourceManager.GetPod(downPod.Namespace, downPod.Name)
	if err != nil {
		println(err.Error())
		return
	}
	if upPod == nil {
		fmt.Println("没有获取到up集群的pod,返回", downPod.Namespace, downPod.Name)
		return
	}
	upPod = upPod.DeepCopy()
	upPod.Spec = downPod.Spec
	upPod.Status = downPod.Status
	p.p.notifier(upPod)
}

func (p *PodEventHandler) OnDelete(obj interface{}) {
	downPod := obj.(*v1.Pod)
	err := p.p.config.ResourceManager.DeletePod(p.ctx, downPod.GetNamespace(), downPod.GetName())
	if (err != nil && errors2.IsNotFound(err)) || err == nil {
		return
	} else {
		println(err.Error())
	}
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
*/
func trimPod(pod *v1.Pod) {
	addDownPodVirtualKubeletLabels(pod)
	trimObjectMeta(&pod.ObjectMeta)
	pod.Spec.NodeName = ""

	vols := []v1.Volume{}
	for _, v := range pod.Spec.Volumes {
		if isDefaultSecret(v.Name) {
			continue
		}
		vols = append(vols, v)
	}
	pod.Spec.Containers = trimContainers(pod.Spec.Containers)
	pod.Spec.InitContainers = trimContainers(pod.Spec.InitContainers)
	pod.Spec.Volumes = vols
	//pod.Status = v1.PodStatus{}
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
