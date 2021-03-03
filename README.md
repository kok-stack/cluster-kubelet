# kok cluster-kubelet

```shell
export VKUBELET_POD_IP=192.168.212.211
export APISERVER_CERT_LOCATION=/mnt/d/code/cluster-kubelet/hack/skaffold/virtual-kubelet/vkubelet-mock-0-crt.pem
export APISERVER_KEY_LOCATION=/mnt/d/code/cluster-kubelet/hack/skaffold/virtual-kubelet/vkubelet-mock-0-key.pem
/mnt/d/code/cluster-kubelet/bin/virtual-kubelet --kubeconfig=/root/.kube/config --down-kubeconfig=/mnt/d/code/cluster-kubelet/examples/config --nodename vkubelet-mock-0 --provider kok --startup-timeout 10s --klog.v "2" --klog.logtostderr --log-level debug

kind create cluster --config kind-config-1.yaml --name c1
cp ~/.kube/config /mnt/d/code/cluster-kubelet/examples/config
kind create cluster --config kind-config-2.yaml --name c2
```

