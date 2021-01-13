package v1alpha1

import (
	"context"
	"encoding/json"
	"github.com/blang/semver"
	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-operator/pkg/scyllaclient"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes"
	"reflect"
	"strconv"
)

func checkValues(c *ScyllaCluster) error {

	ctx := log.WithNewTraceID(context.Background())
	atom := zap.NewAtomicLevelAt(zapcore.DebugLevel)
	logger, _ := log.NewProduction(log.Config{
		Level: atom,
	})

	if len(c.Spec.ScyllaArgs) > 0 {
		version, err := semver.Parse(c.Spec.Version)
		if err == nil && version.LT(ScyllaVersionThatSupportsArgs) {
			return errors.Errorf("ScyllaArgs is only supported starting from %s", ScyllaVersionThatSupportsArgsText)
		}
	}
	rackNames := sets.NewString()

	// Check that all racks are ready before taking any action
	for _, rack := range c.Spec.Datacenter.Racks {
		rackStatus := c.Status.Racks[rack.Name]
		if rackStatus.Members != rackStatus.ReadyMembers {
			logger.Info(ctx, "ks406362 Rack is not ready", "name", rack.Name,
				"members", rackStatus.Members, "ready_members", rackStatus.ReadyMembers)
			return errors.Errorf("Rack %s is not ready for scaling", rack.Name)
		}
	}
	clientset, err := kubernetes.NewForConfig(config.GetConfigOrDie())
	if err != nil {
		return errors.Errorf("ks406362 clientSet creation error %s", err)
	}
	willContain, err := willContainLoad(c, logger, ctx, clientset)
	if err != nil {
		logger.Error(ctx, "ks406362 willContainLoad error", err)
		//return errors.Wrap(err, "Failed check of ability to contain load")

	} else if !willContain {
		logger.Error(ctx, "ks406362 New spec would not contain current load.")
		return errors.Errorf("New spec would not contain current load")
	}

	for _, rack := range c.Spec.Datacenter.Racks {

		// Check that no two racks have the same name
		if rackNames.Has(rack.Name) {
			return errors.Errorf("two racks have the same name: '%s'", rack.Name)
		}
		rackNames.Insert(rack.Name)

		// Check that requested values are not 0
		requests := rack.Resources.Requests
		if requests != nil {
			if requests.Cpu() != nil && requests.Cpu().MilliValue() == 0 {
				return errors.Errorf("requesting 0 cpus is invalid for rack %s", rack.Name)
			}
			if requests.Memory() != nil && requests.Memory().MilliValue() == 0 {
				return errors.Errorf("requesting 0 memory is invalid for rack %s", rack.Name)
			}
			/*if requests.Storage() != nil && requests.Storage().MilliValue() == 0 {
				return errors.Errorf("requesting 0 storage is invalid for rack %s", rack.Name)
			}*/
		}

		// Check that limits are defined
		limits := rack.Resources.Limits
		if limits == nil || limits.Cpu().Value() == 0 || limits.Memory().Value() == 0 {
			return errors.Errorf("set cpu, memory resource limits for rack %s", rack.Name)
		}

		// If the cluster has cpuset
		if c.Spec.CpuSet {
			cores := limits.Cpu().MilliValue()

			// CPU limits must be whole cores
			if cores%1000 != 0 {
				return errors.Errorf("when using cpuset, you must use whole cpu cores, but rack %s has %dm", rack.Name, cores)
			}

			// Requests == Limits and Requests must be set and equal for QOS class guaranteed
			requests := rack.Resources.Requests
			if requests != nil {
				if requests.Cpu().MilliValue() != limits.Cpu().MilliValue() {
					return errors.Errorf("when using cpuset, cpu requests must be the same as cpu limits in rack %s", rack.Name)
				}
				if requests.Memory().MilliValue() != limits.Memory().MilliValue() {
					return errors.Errorf("when using cpuset, memory requests must be the same as memory limits in rack %s", rack.Name)
				}
			} else {
				// Copy the limits
				rack.Resources.Requests = limits.DeepCopy()
			}
		}
	}

	return nil
}

func checkTransitions(old, new *ScyllaCluster) error {
	oldVersion, err := semver.Parse(old.Spec.Version)
	if err != nil {
		return errors.Errorf("invalid old semantic version, err=%s", err)
	}
	newVersion, err := semver.Parse(new.Spec.Version)
	if err != nil {
		return errors.Errorf("invalid new semantic version, err=%s", err)
	}
	// Check that version remained the same
	if newVersion.Major != oldVersion.Major || newVersion.Minor != oldVersion.Minor {
		return errors.Errorf("only upgrading of patch versions are supported")
	}

	// Check that repository remained the same
	if !reflect.DeepEqual(old.Spec.Repository, new.Spec.Repository) {
		return errors.Errorf("repository change is currently not supported, old=%v, new=%v", *old.Spec.Repository, *new.Spec.Repository)
	}

	// Check that sidecarImage remained the same
	if !reflect.DeepEqual(old.Spec.SidecarImage, new.Spec.SidecarImage) {
		return errors.Errorf("change of sidecarImage is currently not supported")
	}

	// Check that the datacenter name didn't change
	if old.Spec.Datacenter.Name != new.Spec.Datacenter.Name {
		return errors.Errorf("change of datacenter name is currently not supported")
	}

	// Check that all rack names are the same as before
	oldRackNames, newRackNames := sets.NewString(), sets.NewString()
	for _, rack := range old.Spec.Datacenter.Racks {
		oldRackNames.Insert(rack.Name)
	}
	for _, rack := range new.Spec.Datacenter.Racks {
		newRackNames.Insert(rack.Name)
	}
	diff := oldRackNames.Difference(newRackNames)
	if diff.Len() != 0 {
		return errors.Errorf("racks %v not found, you cannot remove racks from the spec", diff.List())
	}

	rackMap := make(map[string]RackSpec)
	for _, oldRack := range old.Spec.Datacenter.Racks {
		rackMap[oldRack.Name] = oldRack
	}
	for _, newRack := range new.Spec.Datacenter.Racks {
		oldRack, exists := rackMap[newRack.Name]
		if !exists {
			continue
		}

		// Check that placement is the same as before
		if !reflect.DeepEqual(oldRack.Placement, newRack.Placement) {
			return errors.Errorf("rack %s: changes in placement are not currently supported", oldRack.Name)
		}

		// Check that storage is the same as before
		if !reflect.DeepEqual(oldRack.Storage, newRack.Storage) {
			return errors.Errorf("rack %s: changes in storage are not currently supported", oldRack.Name)
		}
	}

	return nil
}

func willContainLoad(c *ScyllaCluster, logger log.Logger, ctx context.Context, clientset *kubernetes.Clientset) (bool, error) {
	// TODO use function from naming names.go
	namespace := c.ObjectMeta.Namespace
	headless := c.ObjectMeta.Name + "-client"
	clusterName := c.ObjectMeta.Name
	datacenterName := c.Spec.Datacenter.Name

	secrets := clientset.CoreV1().Secrets(namespace)

	conf := scyllaclient.DefaultConfig()
	sc, err := scyllaclient.NewClient(conf, logger)
	if err != nil {
		return false, errors.Errorf("ks406362 create new scylla client error %s", err)
	}

	for _, rack := range c.Spec.Datacenter.Racks {
		scyllaAgentConfig := rack.ScyllaAgentConfig
		secret, err := secrets.Get(context.TODO(), scyllaAgentConfig, metav1.GetOptions{})
		bearerToken := ""
		if err == nil {
			bearerToken = string(secret.Data["token"])

		}
		sc.AddBearerToken(bearerToken)

		rackName := rack.Name
		statusMembers := c.Status.Racks[rackName].Members
		specMembers := rack.Members
		capacityStr := rack.Storage.Capacity
		value, err := resource.ParseQuantity(capacityStr)
		if err != nil {
			return false, errors.Errorf("ks406362 Parsing capacity error %s", err)
		}
		capacity := value.MilliValue()
		loadSum := int64(0)

		for i := 0; i < int(statusMembers); i++ { // old number of members

			svc := clusterName + "-" + datacenterName + "-" + rackName + "-" + strconv.Itoa(i)
			hostName := svc + "." + headless + "." + namespace + ".svc.cluster.local"
			conf.Hosts = []string{hostName}

			ctx = scyllaclient.ForceHostWrapper(ctx, hostName)

			value, err := sc.StorageServiceLoadGetWrapper(ctx)
			if err != nil {
				logger.Error(ctx, "ks406362 StorageServiceLoadGetWrapper", "error", err)
				return false, errors.Errorf("ks406362 StorageServiceLoadGetWrapper error %s", err)
			}
			logger.Info(ctx, "ks406362 getLoad", "name", svc, "value", value, "error", err)
			load, err := value.GetPayload().(json.Number).Int64()
			if err != nil {
				logger.Error(ctx, "ks406362 payload conversion", "error", err)
				return false, errors.Errorf("ks406362 cannot get int from load interface: error %s", err)
			}
			loadSum += load
		}
		logger.Info(ctx, "ks406362 loadSum", "sum", loadSum)

		if loadSum > int64(specMembers)*capacity {
			logger.Error(ctx, "ks406362 unable to store current load in new capacity", "current load", loadSum, "newCapacity", int64(specMembers)*capacity, "error", err)
			return false, nil
		}
	}
	return true, nil
}
