package util

import (
	"context"
	. "github.com/scylladb/scylla-operator/pkg/util/nodeaffinity"

	//	"github.com/scylladb/scylla-operator/pkg/scyllaclient"
	//	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	//	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"go.uber.org/zap/zapcore"
	"k8s.io/client-go/kubernetes"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	scyllav1alpha1 "github.com/scylladb/scylla-operator/pkg/api/v1alpha1"
	"github.com/scylladb/scylla-operator/pkg/naming"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LoggerForCluster returns a logger that will log with context
// about the current cluster
func LoggerForCluster(c *scyllav1alpha1.ScyllaCluster) log.Logger {
	l, _ := log.NewProduction(log.Config{
		Level: zapcore.DebugLevel,
	})
	return l.With("cluster", c.Namespace+"/"+c.Name, "resourceVersion", c.ResourceVersion)
}

// StatefulSetStatusesStale checks if the StatefulSet Objects of a Cluster
// have been observed by the StatefulSet controller.
// If they haven't, their status might be stale, so it's better to wait
// and process them later.
func AreStatefulSetStatusesStale(ctx context.Context, c *scyllav1alpha1.ScyllaCluster, client client.Client) (bool, error) {
	sts := &appsv1.StatefulSet{}
	for _, r := range c.Spec.Datacenter.Racks {
		err := client.Get(ctx, naming.NamespacedName(naming.StatefulSetNameForRack(r, c), c.Namespace), sts)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return true, errors.Wrap(err, "error getting statefulset")
		}
		if sts.Generation != sts.Status.ObservedGeneration {
			return true, nil
		}
	}
	return false, nil
}

func WillSchedule(ctx context.Context, c *scyllav1alpha1.ScyllaCluster, cl client.Client, logger log.Logger) (bool, error) {
	nodes := &corev1.NodeList{}
	if err := cl.List(ctx, nodes); err != nil {
		return false, errors.Wrap(err, "list nodes")
	}
	for _, rack := range c.Spec.Datacenter.Racks { // every rack has to have at least node which taints it tolerates
		hasNodeToBeScheduledOn := false
		for _, node := range nodes.Items {
			taintsPreventScheduling := false
			for _, taint := range node.Spec.Taints { // for every taint
				logger.Debug(ctx, "ks406362 taint: ", "k", taint.Key, "v", taint.Value, "e", taint.Effect)
				if !taintTolerableByRack(ctx, taint, rack, logger) {
					taintsPreventScheduling = true
				}
			}
			nodeAffinityPreventsScheduling := false
			if placement := rack.Placement; placement != nil {
				if nodeAffinity := placement.NodeAffinity; nodeAffinity != nil {
					logger.Debug(ctx, "ks406362 node aff", "affinit", nodeAffinity)
					if nodeSelector := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; nodeSelector != nil {
						_ns, err := NewNodeSelector(nodeSelector)
						if err != nil {
							logger.Error(ctx, "ks406362 NewNodeSelector", "err", err)
						} else if !_ns.Match(&node) {
							logger.Debug(ctx, "ks406362 node selector matches")
							nodeAffinityPreventsScheduling = true
						}
					}
				}
			}
			if !taintsPreventScheduling && !nodeAffinityPreventsScheduling {
				hasNodeToBeScheduledOn = true
			}
		}
		if !hasNodeToBeScheduledOn {
			return false, nil
		}
	}
	return true, nil
}

func taintTolerableByRack(ctx context.Context, taint corev1.Taint, rack scyllav1alpha1.RackSpec, logger log.Logger) bool {
	if taint.Effect == corev1.TaintEffectNoExecute || taint.Effect == corev1.TaintEffectNoSchedule { // that prohibits scheduling/executing
		if rack.Placement != nil {
			for _, toleration := range rack.Placement.Tolerations { // there has to be a matching toleration
				logger.Debug(ctx, "ks406362 tolerations: ", "k", toleration.Key, "v", toleration.Value, "e", toleration.Effect)
				if (toleration.Operator == corev1.TolerationOpExists && (toleration.Key == taint.Key || toleration.Key == "")) || // key has to match
					(toleration.Operator == corev1.TolerationOpEqual && toleration.Key == taint.Key && toleration.Value == taint.Value) { // key, value have to match
					return true //matching toleration
				}
			}
			return false
		} else {
			return false // no Placement means no toleration for taint
		}
	} else {
		return true // taint effect doesn't prohibit scheduling
	}
}

func GetMemberServicesForRack(ctx context.Context, r scyllav1alpha1.RackSpec, c *scyllav1alpha1.ScyllaCluster, cl client.Client) ([]corev1.Service, error) {
	svcList := &corev1.ServiceList{}
	err := cl.List(ctx, svcList, &client.ListOptions{
		LabelSelector: naming.RackSelector(r, c),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list member services")
	}
	return svcList.Items, nil
}

// RefFromString is a helper function that takes a string
// and outputs a reference to that string.
// Useful for initializing a string pointer from a literal.
func RefFromString(s string) *string {
	return &s
}

// RefFromInt32 is a helper function that takes a int32
// and outputs a reference to that int.
func RefFromInt32(i int32) *int32 {
	return &i
}

// VerifyOwner checks if the owner Object is the controller
// of the obj Object and returns an error if it isn't.
func VerifyOwner(obj, owner metav1.Object) error {
	if !metav1.IsControlledBy(obj, owner) {
		ownerRef := metav1.GetControllerOf(obj)
		return errors.Errorf(
			"'%s/%s' is foreign owned: "+
				"it is owned by '%v', not '%s/%s'.",
			obj.GetNamespace(), obj.GetName(),
			ownerRef,
			owner.GetNamespace(), owner.GetName(),
		)
	}
	return nil
}

// NewControllerRef returns an OwnerReference to
// the provided Cluster Object
func NewControllerRef(c *scyllav1alpha1.ScyllaCluster) metav1.OwnerReference {
	return *metav1.NewControllerRef(c, schema.GroupVersionKind{
		Group:   "scylla.scylladb.com",
		Version: "v1alpha1",
		Kind:    "ScyllaCluster",
	})
}

// MarkAsReplaceCandidate patches member service with special label indicating
// that service must be replaced.
func MarkAsReplaceCandidate(ctx context.Context, member *corev1.Service, kubeClient kubernetes.Interface) error {
	if _, ok := member.Labels[naming.ReplaceLabel]; !ok {
		patched := member.DeepCopy()
		patched.Labels[naming.ReplaceLabel] = ""
		if err := PatchService(ctx, member, patched, kubeClient); err != nil {
			return errors.Wrap(err, "patch service as replace")
		}
	}
	return nil
}
