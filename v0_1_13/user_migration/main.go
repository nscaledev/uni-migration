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

	"github.com/spf13/pflag"

	"github.com/unikorn-cloud/core/pkg/constants"
	"github.com/unikorn-cloud/core/pkg/util"
	unikornv1 "github.com/unikorn-cloud/identity/pkg/apis/unikorn/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/kubernetes/scheme"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// migrateUsers goes through each organization scoped user and makes them "global" to the
// identity service, coalescing any multiples.  It replaces the previous user with an
// organization user that is then linked into the groups.
func migrateUsers(ctx context.Context, cli client.Client, users *unikornv1.UserList) error {
	// Pass 1: for every user, build up a set of new users based on a unique
	// subject.
	newUsers := map[string]*unikornv1.User{}

	for i := range users.Items {
		user := &users.Items[i]

		if _, ok := newUsers[user.Spec.Subject]; ok {
			continue
		}

		newUsers[user.Spec.Subject] = &unikornv1.User{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "unikorn-identity",
				Name:      util.GenerateResourceID(),
			},
			Spec: user.Spec,
		}
	}

	for _, user := range newUsers {
		if err := cli.Create(ctx, user); err != nil {
			return err
		}
	}

	// Pass 2: create organization users for each existing user and retain the
	// ID so as to keep group referential consistency.
	for i := range users.Items {
		user := &users.Items[i]

		orgUser := &unikornv1.OrganizationUser{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: user.Namespace,
				Name:      user.Name,
				Labels:    user.Labels,
			},
			Spec: unikornv1.OrganizationUserSpec{
				State: user.Spec.State,
			},
		}

		orgUser.Labels[constants.UserLabel] = newUsers[user.Spec.Subject].Name

		if err := cli.Create(ctx, orgUser); err != nil {
			return err
		}
	}

	// Pass 3: cleanup.
	for i := range users.Items {
		user := &users.Items[i]

		if err := cli.Delete(ctx, user); err != nil {
			return err
		}
	}

	return nil
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

	users := &unikornv1.UserList{}

	if err := cli.List(ctx, users, &client.ListOptions{}); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := migrateUsers(ctx, cli, users); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
