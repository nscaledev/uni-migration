/*
Copyright 2025 the Unikorn Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/spf13/pflag"

	computev1 "github.com/unikorn-cloud/compute/pkg/apis/unikorn/v1alpha1"
	coreclient "github.com/unikorn-cloud/core/pkg/client"
	"github.com/unikorn-cloud/core/pkg/constants"
	"github.com/unikorn-cloud/core/pkg/util"
	identityv1 "github.com/unikorn-cloud/identity/pkg/apis/unikorn/v1alpha1"
	kubernetesv1 "github.com/unikorn-cloud/kubernetes/pkg/apis/unikorn/v1alpha1"
	region "github.com/unikorn-cloud/region/pkg/client"
	regionapi "github.com/unikorn-cloud/region/pkg/openapi"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type upgrader struct {
	client client.Client
	region *regionapi.ClientWithResponses
}

func (u *upgrader) getFlavors(ctx context.Context, organizationID, regionID string) (regionapi.Flavors, error) {
	resp, err := u.region.GetApiV1OrganizationsOrganizationIDRegionsRegionIDFlavorsWithResponse(ctx, organizationID, regionID)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("bad request")
	}

	return *resp.JSON200, nil
}

func (u *upgrader) createComputeAllocation(ctx context.Context, cluster *computev1.ComputeCluster) (*identityv1.Allocation, error) {
	flavors, err := u.getFlavors(ctx, cluster.Labels[constants.OrganizationLabel], cluster.Spec.RegionID)
	if err != nil {
		return nil, err
	}

	var servers int64
	var gpus int64

	for _, pool := range cluster.Spec.WorkloadPools.Pools {
		servers += int64(*pool.Replicas)

		index := slices.IndexFunc(flavors, func(f regionapi.Flavor) bool {
			return f.Metadata.Id == *pool.FlavorID
		})

		if index < 0 {
			return nil, fmt.Errorf("flavor lookup failed")
		}

		flavor := &flavors[index]

		if flavor.Spec.Gpu != nil {
			gpus += int64(flavor.Spec.Gpu.PhysicalCount * *pool.Replicas)
		}
	}

	out := &identityv1.Allocation{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      util.GenerateResourceID(),
			Labels: map[string]string{
				constants.NameLabel:                   constants.UndefinedName,
				constants.OrganizationLabel:           cluster.Labels[constants.OrganizationLabel],
				constants.ProjectLabel:                cluster.Labels[constants.ProjectLabel],
				constants.ReferencedResourceKindLabel: "computecluster",
				constants.ReferencedResourceIDLabel:   cluster.Name,
			},
		},
		Spec: identityv1.AllocationSpec{
			Allocations: []identityv1.ResourceAllocation{
				{
					Kind:      "clusters",
					Committed: resource.NewQuantity(1, resource.DecimalSI),
					Reserved:  resource.NewQuantity(0, resource.DecimalSI),
				},
				{
					Kind:      "servers",
					Committed: resource.NewQuantity(servers, resource.DecimalSI),
					Reserved:  resource.NewQuantity(0, resource.DecimalSI),
				},
				{
					Kind:      "gpus",
					Committed: resource.NewQuantity(gpus, resource.DecimalSI),
					Reserved:  resource.NewQuantity(0, resource.DecimalSI),
				},
			},
		},
	}

	return out, nil
}

func (u *upgrader) createKubernetesClusterAllocation(ctx context.Context, cluster *kubernetesv1.KubernetesCluster) (*identityv1.Allocation, error) {
	flavors, err := u.getFlavors(ctx, cluster.Labels[constants.OrganizationLabel], cluster.Spec.RegionID)
	if err != nil {
		return nil, err
	}

	var serversCommitted int

	var serversReserved int

	var gpusCommitted int

	var gpusReserved int

	for _, pool := range cluster.Spec.WorkloadPools.Pools {
		serversMinimum := *pool.Replicas
		serversMaximum := *pool.Replicas

		if pool.Autoscaling != nil {
			serversMinimum = *pool.Autoscaling.MinimumReplicas
			serversMaximum = *pool.Autoscaling.MaximumReplicas
		}

		reserved := serversMaximum - serversMinimum

		serversCommitted += serversMinimum
		serversReserved += reserved

		index := slices.IndexFunc(flavors, func(f regionapi.Flavor) bool {
			return f.Metadata.Id == *pool.FlavorID
		})

		if index < 0 {
			return nil, fmt.Errorf("flavor lookup failed")
		}

		flavor := &flavors[index]

		if flavor.Spec.Gpu != nil {
			gpusCommitted += serversMinimum * flavor.Spec.Gpu.PhysicalCount
			gpusReserved += reserved * flavor.Spec.Gpu.PhysicalCount
		}
	}

	out := &identityv1.Allocation{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      util.GenerateResourceID(),
			Labels: map[string]string{
				constants.NameLabel:                   constants.UndefinedName,
				constants.OrganizationLabel:           cluster.Labels[constants.OrganizationLabel],
				constants.ProjectLabel:                cluster.Labels[constants.ProjectLabel],
				constants.ReferencedResourceKindLabel: "kubernetescluster",
				constants.ReferencedResourceIDLabel:   cluster.Name,
			},
		},
		Spec: identityv1.AllocationSpec{
			Allocations: []identityv1.ResourceAllocation{
				{
					Kind:      "clusters",
					Committed: resource.NewQuantity(1, resource.DecimalSI),
					Reserved:  resource.NewQuantity(0, resource.DecimalSI),
				},
				{
					Kind:      "servers",
					Committed: resource.NewQuantity(int64(serversCommitted), resource.DecimalSI),
					Reserved:  resource.NewQuantity(int64(serversReserved), resource.DecimalSI),
				},
				{
					Kind:      "gpus",
					Committed: resource.NewQuantity(int64(gpusCommitted), resource.DecimalSI),
					Reserved:  resource.NewQuantity(int64(gpusReserved), resource.DecimalSI),
				},
			},
		},
	}

	return out, nil

}

func (u *upgrader) processQuota(ctx context.Context, quota *identityv1.Quota) error {
	fmt.Println("Creating quota: ", quota)

	return u.client.Create(ctx, quota)
}

func (u *upgrader) processAllocation(ctx context.Context, allocation *identityv1.Allocation) error {
	fmt.Println("Creating allocation: ", allocation)

	return u.client.Create(ctx, allocation)
}

func (u *upgrader) defaultQuota(ctx context.Context, organization *identityv1.Organization) (*identityv1.Quota, error) {
	metadata := &identityv1.QuotaMetadataList{}

	if err := u.client.List(ctx, metadata, &client.ListOptions{}); err != nil {
		return nil, err
	}

	quota := &identityv1.Quota{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: organization.Status.Namespace,
			Name:      util.GenerateResourceID(),
			Labels: map[string]string{
				constants.OrganizationLabel: organization.Name,
				constants.NameLabel:         constants.UndefinedName,
			},
		},
		Spec: identityv1.QuotaSpec{
			Quotas: make([]identityv1.ResourceQuota, len(metadata.Items)),
		},
	}

	for i := range metadata.Items {
		quota.Spec.Quotas[i] = identityv1.ResourceQuota{
			Kind:     metadata.Items[i].Name,
			Quantity: metadata.Items[i].Spec.Default,
		}
	}

	return quota, nil
}

func (u *upgrader) processOrganization(ctx context.Context, organization *identityv1.Organization) error {
	fmt.Println("Processing organization: ", organization.Name)

	orgReq, err := labels.NewRequirement(constants.OrganizationLabel, selection.Equals, []string{organization.Name})
	if err != nil {
		return err
	}

	selector := labels.NewSelector().Add(*orgReq)

	computeClusters := &computev1.ComputeClusterList{}

	if err := u.client.List(ctx, computeClusters, &client.ListOptions{LabelSelector: selector}); err != nil {
		return err
	}

	kubernetesClusters := &kubernetesv1.KubernetesClusterList{}

	if err := u.client.List(ctx, kubernetesClusters, &client.ListOptions{LabelSelector: selector}); err != nil {
		return err
	}

	var allocations identityv1.AllocationList

	for i := range computeClusters.Items {
		allocation, err := u.createComputeAllocation(ctx, &computeClusters.Items[i])
		if err != nil {
			return err
		}

		allocations.Items = append(allocations.Items, *allocation)

		cluster := &computeClusters.Items[i]

		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}

		cluster.Annotations[constants.AllocationAnnotation] = allocation.Name

		if err := u.client.Update(ctx, cluster); err != nil {
			return err
		}
	}

	for i := range kubernetesClusters.Items {
		allocation, err := u.createKubernetesClusterAllocation(ctx, &kubernetesClusters.Items[i])
		if err != nil {
			return err
		}

		allocations.Items = append(allocations.Items, *allocation)

		cluster := &kubernetesClusters.Items[i]

		if cluster.Annotations == nil {
			cluster.Annotations = map[string]string{}
		}

		cluster.Annotations[constants.AllocationAnnotation] = allocation.Name

		if err := u.client.Update(ctx, cluster); err != nil {
			return err
		}
	}

	// Work out the quotas required for the allocations.
	quotas := map[string]*resource.Quantity{}

	for i := range allocations.Items {
		allocation := &allocations.Items[i]

		for _, ra := range allocation.Spec.Allocations {
			if _, ok := quotas[ra.Kind]; !ok {
				quotas[ra.Kind] = resource.NewQuantity(0, resource.DecimalSI)
			}

			quotas[ra.Kind].Add(*ra.Committed)
			quotas[ra.Kind].Add(*ra.Reserved)
		}
	}

	quota, err := u.defaultQuota(ctx, organization)
	if err != nil {
		return err
	}

	// Select the largest of the default or that required by the allocations.
	for kind := range maps.Keys(quotas) {
		index := slices.IndexFunc(quota.Spec.Quotas, func(q identityv1.ResourceQuota) bool {
			return q.Kind == kind
		})

		if index < 0 {
			return fmt.Errorf("quota undefined")
		}

		if quotas[kind].Value() > quota.Spec.Quotas[index].Quantity.Value() {
			quota.Spec.Quotas[index].Quantity = quotas[kind]
		}
	}

	if err := u.processQuota(ctx, quota); err != nil {
		return err
	}

	// Create the allocations.
	for i := range allocations.Items {
		if err := u.processAllocation(ctx, &allocations.Items[i]); err != nil {
			return err
		}
	}

	return nil
}

type accessToken struct {
	token string
}

func (a accessToken) Get() string {
	return a.token
}

func (a *accessToken) AddFlags(f *pflag.FlagSet) {
	f.StringVar(&a.token, "access-token", "", "Access token for API access")
}

func main() {
	ctx := context.Background()

	if err := identityv1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := computev1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := kubernetesv1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	configFlags := genericclioptions.NewConfigFlags(true)
	configFlags.AddFlags(pflag.CommandLine)

	var accessToken accessToken
	accessToken.AddFlags(pflag.CommandLine)

	options := region.NewOptions()
	options.AddFlags(pflag.CommandLine)

	clientOptions := &coreclient.HTTPClientOptions{}
	clientOptions.AddFlags(pflag.CommandLine)

	pflag.Parse()

	if accessToken.Get() == "" {
		fmt.Println("--access-token flag is required, please create a service account")
		os.Exit(1)
	}

	config, err := cmdutil.NewFactory(configFlags).ToRESTConfig()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cli, err := client.New(config, client.Options{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	region, err := region.New(cli, options, clientOptions).Client(ctx, accessToken)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	u := &upgrader{
		client: cli,
		region: region,
	}

	organizations := &identityv1.OrganizationList{}

	if err := cli.List(ctx, organizations, &client.ListOptions{}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := range organizations.Items {
		if err := u.processOrganization(ctx, &organizations.Items[i]); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
