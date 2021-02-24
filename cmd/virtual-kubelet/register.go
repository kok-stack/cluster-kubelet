package main

import (
	"context"
	"github.com/kok-stack/cluster-kubelet/cmd/virtual-kubelet/internal/provider"
	"github.com/kok-stack/cluster-kubelet/cmd/virtual-kubelet/internal/provider/kok"
	"github.com/kok-stack/cluster-kubelet/cmd/virtual-kubelet/internal/provider/mock"
)

func registerMock(s *provider.Store) {
	s.Register("mock", func(cfg provider.InitConfig) (provider.Provider, error) { //nolint:errcheck
		return mock.NewMockProvider(
			cfg.ConfigPath,
			cfg.NodeName,
			cfg.OperatingSystem,
			cfg.InternalIP,
			cfg.DaemonPort,
		)
	})
}

func registerClusterProvider(ctx context.Context, s *provider.Store) {
	if err := s.Register("kok", func(cfg provider.InitConfig) (provider.Provider, error) {
		return kok.NewProvider(ctx, cfg)
	}); err != nil {
		panic(err.Error())
	}
}
