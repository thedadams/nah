package triggers

import (
	"context"
	"fmt"
	"strings"

	"github.com/obot-platform/nah/pkg/backend"
	"github.com/obot-platform/nah/pkg/log"
	"github.com/obot-platform/nah/pkg/untriggered"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type Triggers struct {
	trigger   backend.Trigger
	gvkLookup backend.Backend
	scheme    *runtime.Scheme
	watcher   Watcher
	store     store
}

func NewInMemory(scheme *runtime.Scheme, trigger backend.Trigger, gvkLookup backend.Backend, watcher Watcher) *Triggers {
	return newWithStore(scheme, trigger, gvkLookup, watcher, newInMemoryStore())
}

func NewWithDSN(scheme *runtime.Scheme, trigger backend.Trigger, gvkLookup backend.Backend, watcher Watcher, dsn string) (*Triggers, error) {
	dbStore, err := newDBStore(dsn)
	if err != nil {
		return nil, fmt.Errorf("error creating db store: %v", err)
	}
	return newWithStore(scheme, trigger, gvkLookup, watcher, dbStore), nil
}

func newWithStore(scheme *runtime.Scheme, trigger backend.Trigger, gvkLookup backend.Backend, watcher Watcher, store store) *Triggers {
	return &Triggers{
		trigger:   trigger,
		gvkLookup: gvkLookup,
		scheme:    scheme,
		watcher:   watcher,
		store:     store,
	}
}

func (t *Triggers) Close() error {
	return t.store.Close()
}

func (t *Triggers) Register(ctx context.Context, sourceGVK schema.GroupVersionKind, key string, obj runtime.Object, namespace, name string, selector labels.Selector, fields fields.Selector) (schema.GroupVersionKind, bool, error) {
	if untriggered.IsWrapped(obj) {
		return schema.GroupVersionKind{}, false, nil
	}
	gvk, err := t.gvkLookup.GVKForObject(obj, t.scheme)
	if err != nil {
		return gvk, false, err
	}

	if _, ok := obj.(kclient.ObjectList); ok {
		gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	}

	if key == (types.NamespacedName{Namespace: namespace, Name: name}.String()) {
		return gvk, false, nil
	}

	if err = t.store.RegisterMatcher(ctx, sourceGVK, key, gvk, objectMatcher{
		Namespace: namespace,
		Name:      name,
		Selector:  selector,
		Fields:    fields,
	}); err != nil {
		return gvk, false, err
	}

	return gvk, true, t.watcher.WatchGVK(gvk)
}

func (t *Triggers) Trigger(ctx context.Context, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) {
	if err := t.store.Trigger(ctx, t.trigger, gvk, key, namespace, name, obj); err != nil {
		log.Errorf("Failed to get matchers for %s: %v", gvk, err)
		return
	}
}

// UnregisterAndTrigger will unregister all triggers for the object, both as source and target.
// If a trigger source matches the object exactly, then the trigger will be invoked.
func (t *Triggers) UnregisterAndTrigger(ctx context.Context, gvk schema.GroupVersionKind, key, namespace, name string, obj kclient.Object) {
	if err := t.store.UnregisterMatcher(ctx, t.trigger, gvk, key, namespace, name, obj); err != nil {
		log.Errorf("Failed to unregister matchers for %s: %v", gvk, err)
		return
	}
}

type Watcher interface {
	WatchGVK(gvks ...schema.GroupVersionKind) error
}

type store interface {
	RegisterMatcher(context.Context, schema.GroupVersionKind, string, schema.GroupVersionKind, objectMatcher) error
	Trigger(context.Context, backend.Trigger, schema.GroupVersionKind, string, string, string, kclient.Object) error
	UnregisterMatcher(context.Context, backend.Trigger, schema.GroupVersionKind, string, string, string, kclient.Object) error
	Close() error
}
