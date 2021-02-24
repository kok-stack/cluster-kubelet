package kok

import (
	"context"
	"github.com/kok-stack/cluster-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/remotecommand"
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
	p *Provider
}

func (p *PodEventHandler) OnAdd(obj interface{}) {
	pod := obj.(*v1.Pod)
	p.p.notifier(pod)
}

func (p *PodEventHandler) OnUpdate(oldObj, newObj interface{}) {
	pod := newObj.(*v1.Pod)
	p.p.notifier(pod)
}

func (p *PodEventHandler) OnDelete(obj interface{}) {
	pod := obj.(*v1.Pod)
	p.p.notifier(pod)
}

func (p *Provider) checkAndCreateConfigMap(ctx context.Context, cmName string, namespace string) error {
	_, err := p.downConfigMapLister.ConfigMaps(namespace).Get(cmName)
	//如果不存在则创建
	if err != nil && errors2.IsNotFound(err) {
		//从当前集群获取cm,并在down集群中创建
		upConfigMap, err := p.config.ResourceManager.GetConfigMap(cmName, namespace)
		if err != nil {
			return err
		}
		_, err = p.downClientSet.CoreV1().ConfigMaps(namespace).Create(ctx, upConfigMap, v12.CreateOptions{})
		if err != nil {
			return err
		}
	}
	return err
}

func (p *Provider) checkAndCreateSecret(ctx context.Context, secretName string, namespace string) error {
	_, err := p.downSecretLister.Secrets(namespace).Get(secretName)
	//如果不存在则创建
	if err != nil && errors2.IsNotFound(err) {
		//从当前集群获取cm,并在down集群中创建
		getSecret, err := p.config.ResourceManager.GetSecret(secretName, namespace)
		if err != nil {
			return err
		}
		_, err = p.downClientSet.CoreV1().Secrets(namespace).Create(ctx, getSecret, v12.CreateOptions{})
		if err != nil {
			return err
		}
	}
	return err
}
