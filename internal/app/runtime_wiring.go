package app

import (
	"context"
	"fmt"
	"strings"

	appconfig "aieas_backend/internal/config"
	httptransport "aieas_backend/internal/transport/http"
	wstransport "aieas_backend/internal/transport/ws"
)

func isAllMode(cfg appconfig.Config) bool {
	return cfg.App.Role == "" || cfg.App.Role == "all"
}

func isAPIMode(cfg appconfig.Config) bool {
	return cfg.App.Role == "api"
}

func isWSGatewayMode(cfg appconfig.Config) bool {
	return cfg.App.Role == "ws-gateway"
}

func shouldRegisterAPIRoutes(cfg appconfig.Config) bool {
	return isAllMode(cfg) || isAPIMode(cfg)
}

func shouldRegisterWSRoutes(cfg appconfig.Config) bool {
	return isAllMode(cfg) || isWSGatewayMode(cfg)
}

func shouldStartBusinessWorkers(cfg appconfig.Config) bool {
	return isAllMode(cfg) || isAPIMode(cfg)
}

func shouldStartWSConsumers(cfg appconfig.Config) bool {
	return isAllMode(cfg) || isWSGatewayMode(cfg)
}

func filterReadinessProbesForRole(probes map[string]httptransport.ReadinessProbe, cfg appconfig.Config) map[string]httptransport.ReadinessProbe {
	if len(probes) == 0 || !isWSGatewayMode(cfg) {
		return probes
	}
	out := make(map[string]httptransport.ReadinessProbe, len(probes))
	for name, probe := range probes {
		if isWSGatewayReadinessProbe(name) {
			out[name] = probe
		}
	}
	return out
}

func isWSGatewayReadinessProbe(name string) bool {
	switch name {
	case "redis_rt", "redis_cache", "scripts", "ws_draining":
		return true
	}
	return strings.Contains(name, "pubsub") || strings.Contains(name, "pub_sub") || strings.Contains(name, "stream")
}

func withWSDrainingReadinessProbe(probes map[string]httptransport.ReadinessProbe, hub *wstransport.Hub, cfg appconfig.Config) map[string]httptransport.ReadinessProbe {
	if hub == nil || !shouldRegisterWSRoutes(cfg) {
		return probes
	}
	out := make(map[string]httptransport.ReadinessProbe, len(probes)+1)
	for name, probe := range probes {
		out[name] = probe
	}
	out["ws_draining"] = func(ctx context.Context) error {
		_ = ctx
		if hub.IsDraining() {
			return fmt.Errorf("websocket gateway draining")
		}
		return nil
	}
	return out
}
