package kok

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kok-stack/cluster-kubelet/cmd/virtual-kubelet/internal/provider"
	"github.com/kok-stack/cluster-kubelet/node/api"
	"github.com/kok-stack/cluster-kubelet/trace"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	stats "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/clientset/versioned"
	"os"
	"time"
)

//TODO:demo 配置文件
type config struct {
	provider.InitConfig
	DownKubeConfig string `json:"down_kube_config"`
}

//TODO:tracing
type Provider struct {
	config               *config
	startTime            time.Time
	notifier             func(*v1.Pod)
	downConfig           *rest.Config
	nodeLister           corev1listers.NodeLister
	podLister            corev1listers.PodLister
	downConfigMapLister  corev1listers.ConfigMapLister
	downSecretLister     corev1listers.SecretLister
	downClientSet        *kubernetes.Clientset
	downMetricsClientSet *versioned.Clientset
}

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

func (p *Provider) NotifyPods(ctx context.Context, f func(*v1.Pod)) {
	p.notifier = f
}

func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	namespace := pod.Namespace
	secrets := getSecrets(pod)
	for s := range secrets {
		if err := p.checkAndCreateSecret(ctx, s, namespace); err != nil {
			return err
		}
	}
	configMaps := getConfigMaps(pod)
	for s := range configMaps {
		if err := p.checkAndCreateConfigMap(ctx, s, namespace); err != nil {
			return err
		}
	}

	_, err := p.downClientSet.CoreV1().Pods(pod.GetNamespace()).Create(ctx, pod, v12.CreateOptions{})
	return err
}

func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	_, err := p.downClientSet.CoreV1().Pods(pod.GetNamespace()).Update(ctx, pod, v12.UpdateOptions{})
	return err
}

func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	return p.downClientSet.CoreV1().Pods(pod.GetNamespace()).Delete(ctx, pod.GetName(), v12.DeleteOptions{})
}

func (p *Provider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	return p.podLister.Pods(namespace).Get(name)
}

func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	return p.podLister.List(labels.Everything())
	//list, err := p.downClientSet.CoreV1().Pods("").List(ctx, v12.ListOptions{})
	//if err != nil {
	//	return nil, err
	//}
	//pods := make([]*v1.Pod, len(list.Items))
	//for i, item := range list.Items {
	//	pods[i] = &item
	//}
	//return pods, nil
}

func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
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
	return logs.Stream(ctx)
}

func (p *Provider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
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
		return fmt.Errorf("could not make remote command: %v", err)
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
		return err
	}

	return nil
}

func (p *Provider) ConfigureNode(ctx context.Context, node *v1.Node) {
	ctx, span := trace.StartSpan(ctx, "ConfigureNode")
	defer span.End()

	nodes, err := p.nodeLister.List(labels.Nothing())
	if err != nil {
		return
	}

	allocates := make([]v1.ResourceList, len(nodes))
	capacitys := make([]v1.ResourceList, len(nodes))
	for i, n := range nodes {
		allocates[i] = n.Status.Allocatable
		capacitys[i] = n.Status.Capacity
	}
	node.Status.Allocatable = totalResourceList(allocates)
	node.Status.Capacity = totalResourceList(allocates)

	//TODO:version获取
	//node.Status.NodeInfo.KubeletVersion = v.version
	node.Status.Addresses = []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: os.Getenv("VKUBELET_POD_IP")}}
	node.Status.Conditions = nodeConditions()
	node.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.config.DaemonPort,
		},
	}
	node.Status.NodeInfo.OperatingSystem = "linux"
	node.Status.NodeInfo.Architecture = "amd64"
	node.ObjectMeta.Labels[v1.LabelArchStable] = "amd64"
	node.ObjectMeta.Labels[v1.LabelOSStable] = "linux"
	node.ObjectMeta.Labels["alpha.service-controller.kubernetes.io/exclude-balancer"] = "true"
	node.ObjectMeta.Labels["node.kubernetes.io/exclude-from-external-load-balancers"] = "true"
}

func (p *Provider) start(ctx context.Context) error {
	c, metricsClientSet, clientset, err := clientSetFromEnv(p.config.DownKubeConfig)
	if err != nil {
		return err
	}
	p.downClientSet = clientset
	p.downConfig = c
	p.downMetricsClientSet = metricsClientSet
	factory := informers.NewSharedInformerFactory(clientset, time.Minute)
	p.nodeLister = factory.Core().V1().Nodes().Lister()
	p.podLister = factory.Core().V1().Pods().Lister()
	factory.Core().V1().Nodes().Informer().AddEventHandler(&NodeEventHandler{p})
	factory.Core().V1().Pods().Informer().AddEventHandler(&PodEventHandler{p})
	p.downConfigMapLister = factory.Core().V1().ConfigMaps().Lister()
	p.downSecretLister = factory.Core().V1().Secrets().Lister()
	factory.Start(ctx.Done())
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
	c := &config{}
	file, err := ioutil.ReadFile(cfg.ConfigPath)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(file, c); err != nil {
		return nil, err
	}
	c.InitConfig = cfg

	p := &Provider{
		config:    c,
		startTime: time.Now(),
		notifier:  nil,
	}
	if err = p.start(ctx); err != nil {
		return nil, err
	}
	return p, nil
}