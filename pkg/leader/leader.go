package leader

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/obot-platform/nah/pkg/log"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	defaultLeaderTTL = time.Minute
	devLeaderTTL     = time.Hour
)

type OnLeader func(context.Context) error
type OnNewLeader func(string)

type ElectionConfig struct {
	TTL                               time.Duration
	Name, Namespace, ResourceLockType string
	restCfg                           *rest.Config
}

func NewDefaultElectionConfig(namespace, name string, cfg *rest.Config) *ElectionConfig {
	ttl := defaultLeaderTTL
	if os.Getenv("BAAAH_DEV_MODE") != "" {
		ttl = devLeaderTTL
	}
	return &ElectionConfig{
		TTL:              ttl,
		Namespace:        namespace,
		Name:             name,
		ResourceLockType: resourcelock.LeasesResourceLock,
		restCfg:          cfg,
	}
}

func NewElectionConfig(ttl time.Duration, namespace, name, lockType string, cfg *rest.Config) *ElectionConfig {
	return &ElectionConfig{
		TTL:              ttl,
		Namespace:        namespace,
		Name:             name,
		ResourceLockType: lockType,
		restCfg:          cfg,
	}
}

func (ec *ElectionConfig) Run(ctx context.Context, id string, onLeader OnLeader, onSwitchLeader OnNewLeader, signalDone chan struct{}) error {
	if ec == nil {
		// Don't start leader election if there is no config.
		return onLeader(ctx)
	}

	if ec.Namespace == "" {
		ec.Namespace = "kube-system"
	}

	if err := ec.run(ctx, id, onLeader, onSwitchLeader, signalDone); err != nil {
		return fmt.Errorf("failed to start leader election for %s: %v", ec.Name, err)
	}

	return nil
}

func (ec *ElectionConfig) run(ctx context.Context, id string, cb OnLeader, onSwitchLeader OnNewLeader, signalDone chan struct{}) error {
	rl, err := resourcelock.NewFromKubeconfig(
		ec.ResourceLockType,
		ec.Namespace,
		ec.Name,
		resourcelock.ResourceLockConfig{
			Identity: id,
		},
		ec.restCfg,
		ec.TTL/2,
	)
	if err != nil {
		return fmt.Errorf("error creating leader lock for %s: %v", ec.Name, err)
	}

	// Catch these signals to ensure a graceful shutdown and leader election release.
	sigCtx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
	defer func() {
		if err != nil {
			// If we encountered an error, cancel the context because we won't be using it.
			cancel()
		}
	}()

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: ec.TTL,
		RenewDeadline: ec.TTL / 2,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				if err := cb(ctx); err != nil {
					log.Fatalf("leader callback error: %v", err)
				}
			},
			OnNewLeader: onSwitchLeader,
			OnStoppedLeading: func() {
				select {
				case <-sigCtx.Done():
					// Must cancel so that the registered signals are no longer caught.
					cancel()

					// The context has been canceled or is otherwise complete.
					// This is a request to terminate. Exit 0.
					// Exiting cleanly is useful when the context is canceled
					// so that Kubernetes doesn't record it exiting in error
					// when the exit was requested. For example, the wrangler-cli
					// package sets up a context that cancels when SIGTERM is
					// sent in. If a node is shut down this is the type of signal
					// sent. In that case you want the 0 exit code to mark it as
					// complete so that everything comes back up correctly after
					// a restart.
					// The pattern found here can be found inside the kube-scheduler.
					log.Infof("requested to terminate, exiting")
					close(signalDone)
				default:
					log.Fatalf("leader election lost for %s", ec.Name)
				}
			},
		},
		ReleaseOnCancel: true,
	})
	if err != nil {
		return err
	}

	go func() {
		le.Run(sigCtx)
	}()
	return nil
}
