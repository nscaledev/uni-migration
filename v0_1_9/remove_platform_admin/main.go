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
	"os"
	"slices"

	"github.com/spf13/pflag"

	"github.com/unikorn-cloud/core/pkg/constants"
	unikornv1 "github.com/unikorn-cloud/identity/pkg/apis/unikorn/v1alpha1"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

func processGroup(ctx context.Context, cli client.Client, group *unikornv1.Group, roles *unikornv1.RoleList) error {
	fmt.Println("Processing group:", group.Name)

	index := slices.IndexFunc(roles.Items, func(role unikornv1.Role) bool {
		return role.Labels[constants.NameLabel] == "platform-administrator"
	})

	if index < 0 {
		fmt.Println("no platform-administrator role detected")
		return nil
	}

	platformAdminRole := &roles.Items[index]

	if !slices.Contains(group.Spec.RoleIDs, platformAdminRole.Name) {
		fmt.Println("group doesn't include the platform-administrator role")
		return nil
	}

	index = slices.IndexFunc(roles.Items, func(role unikornv1.Role) bool {
		return role.Labels[constants.NameLabel] == "administrator"
	})

	if index < 0 {
		fmt.Println("no administrator role detected")
		return nil
	}

	adminRole := &roles.Items[index]

	// Delete the platform-administrator role and add a plain admin one.
	group.Spec.RoleIDs = append(group.Spec.RoleIDs, adminRole.Name)

	slices.Sort(group.Spec.RoleIDs)
	group.Spec.RoleIDs = slices.Compact(group.Spec.RoleIDs)

	group.Spec.RoleIDs = slices.DeleteFunc(group.Spec.RoleIDs, func(id string) bool {
		return id == platformAdminRole.Name
	})

	group.Labels[constants.NameLabel] = "administrators"

	if group.Annotations == nil {
		group.Annotations = map[string]string{}
	}

	group.Annotations[constants.DescriptionAnnotation] = "Automatically migrated group"

	fmt.Println("Updating group:", group)

	return cli.Update(ctx, group)
}

func main() {
	ctx := context.Background()

	if err := unikornv1.AddToScheme(scheme.Scheme); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	configFlags := genericclioptions.NewConfigFlags(true)
	configFlags.AddFlags(pflag.CommandLine)

	pflag.Parse()

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

	roles := &unikornv1.RoleList{}

	if err := cli.List(ctx, roles, &client.ListOptions{}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	groups := &unikornv1.GroupList{}

	if err := cli.List(ctx, groups, &client.ListOptions{}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := range groups.Items {
		if err := processGroup(ctx, cli, &groups.Items[i], roles); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
