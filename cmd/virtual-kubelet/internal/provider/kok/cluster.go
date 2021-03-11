package kok

import (
	"context"
	"fmt"
	"github.com/kok-stack/cluster-kubelet/cmd/virtual-kubelet/internal/provider"
	"github.com/kok-stack/cluster-kubelet/log"
	"github.com/kok-stack/cluster-kubelet/node/api"
	"github.com/kok-stack/cluster-kubelet/trace"
	"github.com/pkg/errors"
	"io"
	v1 "k8s.io/api/core/v1"
	errors2 "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/metrics/pkg/client/clientset/versioned"
	"os"
	"time"
)

const (
	namespaceKey     = "namespace"
	nameKey          = "name"
	containerNameKey = "containerName"
	nodeNameKey      = "nodeName"
)

type Provider struct {
	config               provider.InitConfig
	startTime            time.Time
	downConfig           *rest.Config
	downNodeLister       corev1listers.NodeLister
	downPodLister        corev1listers.PodLister
	downConfigMapLister  corev1listers.ConfigMapLister
	downSecretLister     corev1listers.SecretLister
	downClientSet        *kubernetes.Clientset
	downMetricsClientSet *versioned.Clientset
	downNamespaceLister  corev1listers.NamespaceLister
	podHandler           *PodEventHandler
	nodeHandler          *NodeEventHandler
}

func (p *Provider) NotifyPods(ctx context.Context, f func(*v1.Pod)) {
	p.podHandler.notifyFunc = f
	p.podHandler.start(ctx)
}

func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	ctx, span := trace.StartSpan(ctx, "创建pod")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, pod.Namespace, nameKey, pod.Name, nodeNameKey, p.config.NodeName)
	log.G(ctx).Info("创建pod")

	namespace := pod.Namespace
	secrets := getSecrets(pod)
	configMaps := getConfigMaps(pod)
	timeCtx, cancelFunc := context.WithTimeout(ctx, time.Second*10)
	defer cancelFunc()
	if err2 := wait.PollImmediateUntil(time.Microsecond*100, func() (done bool, err error) {
		if err := p.syncNamespaces(ctx, namespace); err != nil {
			return false, err
		}
		//TODO:serviceAccount,pvc处理
		//TODO:如何处理pod依赖的对象 serviceAccount-->role-->rolebinding,pvc-->pv-->storageClass,以及其他一些隐试依赖
		for s := range secrets {
			if err := p.syncSecret(ctx, s, namespace); err != nil {
				return false, err
			}
		}
		for s := range configMaps {
			if err := p.syncConfigMap(ctx, s, namespace); err != nil {
				return false, err
			}
		}
		return true, nil
	}, timeCtx.Done()); err2 != nil {
		return err2
	}

	trimPod(pod, p.config.NodeName)

	//marshal, _ := json.Marshal(pod)
	//log.G(ctx).Debugf("pod内容为:", string(marshal))

	_, err := p.downClientSet.CoreV1().Pods(pod.GetNamespace()).Create(ctx, pod, v12.CreateOptions{})
	if err != nil {
		log.G(ctx).Warnf("创建pod失败", err)
		span.SetStatus(err)
	}
	return err
}

//
//func setPodEnvironmentVariableValue(pod *v1.Pod, config provider.InitConfig) {
//	for idx := range pod.Spec.InitContainers {
//		setContainerEnvironmentVariableValue(&pod.Spec.InitContainers[idx], config)
//	}
//	for idx := range pod.Spec.Containers {
//		setContainerEnvironmentVariableValue(&pod.Spec.Containers[idx], config)
//	}
//}
//
//func setContainerEnvironmentVariableValue(c *v1.Container, config provider.InitConfig) {
//	for _, envVar := range c.Env {
//		if envVar.ValueFrom == nil || envVar.ValueFrom.FieldRef == nil || len(envVar.ValueFrom.FieldRef.FieldPath) == 0 {
//			continue
//		}
//		switch envVar.ValueFrom.FieldRef.FieldPath{
//		case "status.hostIP":
//
//		}
//	}
//}

func (p *Provider) syncNamespaces(ctx context.Context, namespace string) error {
	upNs, err := p.config.ResourceManager.GetNamespace(namespace)
	if err != nil {
		return err
	}
	if _, err := p.downNamespaceLister.Get(namespace); err != nil {
		if !errors2.IsNotFound(err) {
			return err
		}
		if _, createErr := p.downClientSet.CoreV1().Namespaces().Create(ctx, &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name:        namespace,
				Labels:      upNs.Labels,
				Annotations: upNs.Annotations,
			},
		}, v12.CreateOptions{}); createErr != nil && errors2.IsAlreadyExists(createErr) {
			return err
		}
	}
	return nil
}

func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	//up-->down
	ctx, span := trace.StartSpan(ctx, "更新pod")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, pod.Namespace, nameKey, pod.Name, nodeNameKey, p.config.NodeName)

	trimPod(pod, p.config.NodeName)
	_, err := p.downClientSet.CoreV1().Pods(pod.GetNamespace()).Update(ctx, pod, v12.UpdateOptions{})
	if err != nil {
		span.SetStatus(err)
	}
	return err
}

func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	//up-->down
	ctx, span := trace.StartSpan(ctx, "删除pod")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, pod.Namespace, nameKey, pod.Name, nodeNameKey, p.config.NodeName)

	err := p.downClientSet.CoreV1().Pods(pod.GetNamespace()).Delete(ctx, pod.GetName(), v12.DeleteOptions{})
	if (err != nil && errors2.IsNotFound(err)) || err == nil {
		return nil
	} else {
		span.SetStatus(err)
	}
	return err
}

func (p *Provider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	ctx, span := trace.StartSpan(ctx, "获取pod")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, namespace, nameKey, name, nodeNameKey, p.config.NodeName)

	pod, err := p.config.ResourceManager.GetPod(namespace, name)
	if err != nil {
		span.SetStatus(err)
		return nil, err
	}
	down, err := p.downPodLister.Pods(namespace).Get(name)
	if err != nil {
		span.SetStatus(err)
		return nil, err
	}
	pod.Status = down.Status
	return pod, nil
}

func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	ctx, span := trace.StartSpan(ctx, "获取pod状态")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, namespace, nameKey, name, nodeNameKey, p.config.NodeName)

	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		span.SetStatus(err)
		return nil, err
	}
	return &pod.Status, nil
}

func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	ctx, span := trace.StartSpan(ctx, "获取pod列表")
	defer span.End()
	ctx = addAttributes(ctx, span, nodeNameKey, p.config.NodeName)

	//获取down集群的virtual-kubelet的pod,然后转换status到up集群
	list, err := p.downPodLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	pods := make([]*v1.Pod, len(list))
	for i, pod := range list {
		getPod, err := p.config.ResourceManager.GetPod(pod.Namespace, pod.Name)
		if err != nil {
			span.Logger().WithField(namespaceKey, pod.Namespace).WithField(nameKey, pod.Name).Error(err)
			span.SetStatus(err)
			return nil, err
		}
		getPod.Status = pod.Status
		pods[i] = getPod
	}
	return pods, nil
}

func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	ctx, span := trace.StartSpan(ctx, "获取pod日志")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, namespace, nameKey, podName, nodeNameKey, p.config.NodeName)

	tailLine := int64(opts.Tail)
	limitBytes := int64(opts.LimitBytes)
	sinceSeconds := opts.SinceSeconds
	options := &v1.PodLogOptions{
		Container:  containerName,
		Timestamps: opts.Timestamps,
		Follow:     opts.Follow,
	}
	if tailLine != 0 {
		options.TailLines = &tailLine
	}
	if limitBytes != 0 {
		options.LimitBytes = &limitBytes
	}
	if !opts.SinceTime.IsZero() {
		*options.SinceTime = metav1.Time{Time: opts.SinceTime}
	}
	if sinceSeconds != 0 {
		*options.SinceSeconds = int64(sinceSeconds)
	}
	if opts.Previous {
		options.Previous = opts.Previous
	}
	if opts.Follow {
		options.Follow = opts.Follow
	}

	logs := p.downClientSet.CoreV1().Pods(namespace).GetLogs(podName, options)
	stream, err := logs.Stream(ctx)
	if err != nil {
		span.SetStatus(err)
	}
	return stream, err
}

func (p *Provider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
	ctx, span := trace.StartSpan(ctx, "在pod中执行命令")
	defer span.End()
	ctx = addAttributes(ctx, span, namespaceKey, namespace, nameKey, podName, nodeNameKey, p.config.NodeName)

	defer func() {
		if attach.Stdout() != nil {
			attach.Stdout().Close()
		}
		if attach.Stderr() != nil {
			attach.Stderr().Close()
		}
	}()

	req := p.downClientSet.CoreV1().RESTClient().
		Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("exec").
		Timeout(0).
		VersionedParams(&v1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdin:     attach.Stdin() != nil,
			Stdout:    attach.Stdout() != nil,
			Stderr:    attach.Stderr() != nil,
			TTY:       attach.TTY(),
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(p.downConfig, "POST", req.URL())
	if err != nil {
		err := fmt.Errorf("could not make remote command: %v", err)
		span.SetStatus(err)
		return err
	}

	ts := &termSize{attach: attach}

	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:             attach.Stdin(),
		Stdout:            attach.Stdout(),
		Stderr:            attach.Stderr(),
		Tty:               attach.TTY(),
		TerminalSizeQueue: ts,
	})
	if err != nil {
		span.SetStatus(err)
		return err
	}

	return nil
}

func (p *Provider) ConfigureNode(ctx context.Context, node *v1.Node) {
	p.nodeHandler.configureNode(ctx, node)
}

func (p *Provider) start(ctx context.Context) error {
	c, metricsClientSet, clientset, err := clientSetFromEnv(p.config.DownKubeConfigPath)
	if err != nil {
		return err
	}
	p.downClientSet = clientset
	p.downConfig = c
	p.downMetricsClientSet = metricsClientSet

	nodeFactory := informers.NewSharedInformerFactory(clientset, time.Minute)
	p.downNodeLister = nodeFactory.Core().V1().Nodes().Lister()
	p.downNamespaceLister = nodeFactory.Core().V1().Namespaces().Lister()
	p.downConfigMapLister = nodeFactory.Core().V1().ConfigMaps().Lister()
	p.downSecretLister = nodeFactory.Core().V1().Secrets().Lister()

	factory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Minute, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
		options.LabelSelector = getDownPodVirtualKubeletLabels()
	}))
	p.downPodLister = factory.Core().V1().Pods().Lister()
	p.nodeHandler = &NodeEventHandler{ctx: ctx, p: p, updateNode: make(chan struct{}), downNodeLister: p.downNodeLister, downPodLister: p.downPodLister}
	p.podHandler = &PodEventHandler{ctx: ctx, p: p, podUpdateCh: make(chan *v1.Pod), nodeHandler: p.nodeHandler}
	factory.Core().V1().Pods().Informer().AddEventHandler(p.podHandler)
	factory.Start(ctx.Done())
	nodeFactory.Core().V1().Nodes().Informer().AddEventHandler(p.nodeHandler)
	nodeFactory.Start(ctx.Done())
	return nil
}

// ClientsetFromEnv returns a kuberentes client set from:
// 1. the passed in kubeconfig path
// 2. If the kubeconfig path is empty or non-existent, then the in-cluster config is used.
func clientSetFromEnv(kubeConfigPath string) (*rest.Config, *versioned.Clientset, *kubernetes.Clientset, error) {
	var (
		config *rest.Config
		err    error
	)

	if kubeConfigPath != "" {
		config, err = clientSetFromEnvKubeConfigPath(kubeConfigPath)
	} else {
		config, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "error getting rest client config")
	}

	metricsClient, err := versioned.NewForConfig(config)
	clientset, err := kubernetes.NewForConfig(config)
	return config, metricsClient, clientset, err
}

func clientSetFromEnvKubeConfigPath(kubeConfigPath string) (*rest.Config, error) {
	_, err := os.Stat(kubeConfigPath)
	if os.IsNotExist(err) {
		return rest.InClusterConfig()
	}
	if err != nil {
		return nil, err
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeConfigPath},
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}

func NewProvider(ctx context.Context, cfg provider.InitConfig) (*Provider, error) {
	p := &Provider{
		config:    cfg,
		startTime: time.Now(),
	}
	if err := p.start(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func addAttributes(ctx context.Context, span trace.Span, attrs ...string) context.Context {
	if len(attrs)%2 == 1 {
		return ctx
	}
	for i := 0; i < len(attrs); i += 2 {
		ctx = span.WithField(ctx, attrs[i], attrs[i+1])
	}
	return ctx
}
