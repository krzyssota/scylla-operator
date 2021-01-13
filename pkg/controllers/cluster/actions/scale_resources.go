package actions

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	scyllav1alpha1 "github.com/scylladb/scylla-operator/pkg/api/v1alpha1"
	"github.com/scylladb/scylla-operator/pkg/controllers/cluster/util"
	"github.com/scylladb/scylla-operator/pkg/naming"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const RackScaleResourcesAction = "rack-scale-resources"

// Implements Action interface
var _ Action = &RackScaleResources{}

type RackScaleResources struct {
	Rack    scyllav1alpha1.RackSpec
	Cluster *scyllav1alpha1.ScyllaCluster
}

func NewRackScaleResourcesAction(r scyllav1alpha1.RackSpec, c *scyllav1alpha1.ScyllaCluster) *RackScaleResources {
	return &RackScaleResources{
		Rack:    r,
		Cluster: c,
	}
}

func (a *RackScaleResources) Name() string {
	return RackScaleResourcesAction
}

func (a *RackScaleResources) Execute(ctx context.Context, s *State) error {
	r, c := a.Rack, a.Cluster

	s.recorder.Event(c, corev1.EventTypeNormal, naming.SuccessSynced,
		fmt.Sprintf("ks406362 saved rack %s to be scaled has limits limits %d %d",
			r.Name,
			r.Resources.Limits.Cpu().Value(), r.Resources.Requests.Memory().Value()))

	sts := &appsv1.StatefulSet{}
	err := s.Get(ctx, naming.NamespacedName(naming.StatefulSetNameForRack(r, c), c.Namespace), sts)
	if err != nil {
		return errors.Wrap(err, "failed to get statefulset")
	}

	idx, err := naming.FindScyllaContainer(sts.Spec.Template.Spec.Containers)
	if err != nil {
		s.recorder.Event(c, corev1.EventTypeWarning, naming.ErrSyncFailed, fmt.Sprintf("ks406362 Error trying to find container named scylla to scale it's resources"))
		return errors.WithStack(err)
	}
	if !reflect.DeepEqual(sts.Spec.Template.Spec.Containers[idx].Resources, r.Resources) { // has resources changed
		err = util.ScaleStatefulSetsResources(ctx, sts, r.Resources, s.kubeclient, c, s.recorder)
		if err != nil {
			return errors.Wrap(err, "ks406362 error trying to scale statefulset's resources")
		}
		// Record event for successful scale-up
		s.recorder.Event(c, corev1.EventTypeNormal, naming.SuccessSynced, fmt.Sprintf("ks406362 Rack %s has resources scaled", r.Name))
		return nil
	} else {
		s.recorder.Event(c, corev1.EventTypeNormal, naming.SuccessSynced, fmt.Sprintf("ks406362 Rack %s - resources haven't been scaled ", r.Name))
		return nil
	}

}
