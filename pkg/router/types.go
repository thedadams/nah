package router

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kclient "sigs.k8s.io/controller-runtime/pkg/client"
)

type AddToSchemer func(s *runtime.Scheme) error

type Handler interface {
	Handle(req Request, resp Response) error
}

type Middleware func(h Handler) Handler

type HandlerFunc func(req Request, resp Response) error

// ErrorHandler is a user defined function to handle an error. If the
// ErrorHandler returns nil this req is considered handled and will not
// be re-enqueued.  If a non-nil resp is return this key will be
// re-enqueued. ErrorHandler will be call for nil errors also so
// That the ErrorHandler can possibly clear a previous error state.
type ErrorHandler func(req Request, resp Response, err error) error

func (h HandlerFunc) Handle(req Request, resp Response) error {
	return h(req, resp)
}

type Request struct {
	Client      kclient.WithWatch
	Object      kclient.Object
	Ctx         context.Context
	GVK         schema.GroupVersionKind
	Namespace   string
	Name        string
	Key         string
	FromTrigger bool
}

func (r *Request) WithContext(ctx context.Context) Request {
	newRequest := *r
	newRequest.Ctx = ctx
	return newRequest
}

func (r *Request) List(object kclient.ObjectList, opts *kclient.ListOptions) error {
	return r.Client.List(r.Ctx, object, opts)
}

func (r *Request) Get(object kclient.Object, namespace, name string) error {
	return r.Client.Get(r.Ctx, Key(namespace, name), object)
}

func (r *Request) Delete(object kclient.Object) error {
	err := r.Client.Delete(r.Ctx, object)
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

type Response interface {
	Attributes() map[string]any
	RetryAfter(delay time.Duration)
}

func Key(namespace, name string) kclient.ObjectKey {
	return kclient.ObjectKey{
		Name:      name,
		Namespace: namespace,
	}
}
