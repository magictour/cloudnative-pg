/*
Copyright The CloudNativePG Contributors

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

package e2e

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/cloudnative-pg/cloudnative-pg/tests"
	"github.com/cloudnative-pg/cloudnative-pg/tests/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("nodeSelector", func() {
	const level = tests.Low

	BeforeEach(func() {
		if testLevelEnv.Depth < int(level) {
			Skip("Test depth is lower than the amount requested for this test")
		}
	})

	Context("The label doesn't exist", func() {
		const namespace = "nodeselector-e2e-missing-label"
		const sampleFile = fixturesDir + "/nodeselector/nodeselector-label-not-exists.yaml"
		const clusterName = "postgresql-nodeselector"
		JustAfterEach(func() {
			if CurrentSpecReport().Failed() {
				env.DumpClusterEnv(namespace, clusterName,
					"out/"+CurrentSpecReport().LeafNodeText+".log")
			}
		})
		AfterEach(func() {
			err := env.DeleteNamespace(namespace)
			Expect(err).ToNot(HaveOccurred())
		})
		It("verifies that pods can't be scheduled", func() {
			// We create a namespace and verify it exists
			By(fmt.Sprintf("having a %v namespace", namespace), func() {
				err := env.CreateNamespace(namespace)
				Expect(err).ToNot(HaveOccurred())

				// Creating a namespace should be quick
				timeout := 20
				namespacedName := types.NamespacedName{
					Namespace: namespace,
					Name:      namespace,
				}
				Eventually(func() (string, error) {
					namespaceResource := &corev1.Namespace{}
					err := env.Client.Get(env.Ctx, namespacedName, namespaceResource)
					return namespaceResource.GetName(), err
				}, timeout).Should(BeEquivalentTo(namespace))
			})

			By(fmt.Sprintf("creating a cluster in the %v namespace", namespace), func() {
				CreateResourceFromFile(namespace, sampleFile)
			})

			// The cluster should be created but the pods shouldn't be scheduled
			// We expect the operator to create the first pod and for that pod
			// to be stuck forever due to affinity issues.
			// We check the error to verify that's the case
			By("verifying that the pods can't be scheduled", func() {
				timeout := 120
				Eventually(func() bool {
					isPending := false
					podList, err := env.GetPodList(namespace)
					Expect(err).ToNot(HaveOccurred())
					if len(podList.Items) > 0 {
						if len(podList.Items[0].Status.Conditions) > 0 {
							if podList.Items[0].Status.Phase == "Pending" && strings.Contains(podList.Items[0].Status.Conditions[0].Message,
								"didn't match") {
								isPending = true
							} else {
								// should never happen, but useful once it happens
								GinkgoWriter.Printf("Found pod in node with status %s and message %s\n",
									podList.Items[0].Status.Phase,
									podList.Items[0].Status.Conditions[0].Message)
							}
						}
					}
					return isPending
				}, timeout).Should(BeEquivalentTo(true))
			})
		})
	})

	Context("The label exists", func() {
		const namespace = "nodeselector-e2e-existing-label"
		const sampleFile = fixturesDir + "/nodeselector/nodeselector-label-exists.yaml"
		const clusterName = "postgresql-nodeselector"
		JustAfterEach(func() {
			if CurrentSpecReport().Failed() {
				env.DumpClusterEnv(namespace, clusterName,
					"out/"+CurrentSpecReport().LeafNodeText+".log")
			}
		})
		AfterEach(func() {
			err := env.DeleteNamespace(namespace)
			Expect(err).ToNot(HaveOccurred())
		})

		It("verifies the pods run on the labeled node", func() {
			var nodeName string
			// Create a cluster in a namespace we'll delete after the test
			err := env.CreateNamespace(namespace)
			Expect(err).ToNot(HaveOccurred())

			// We label one node with the label we have defined in the cluster
			// YAML definition
			By("labelling a node", func() {
				nodeList, err := env.GetNodeList()
				Expect(err).ToNot(HaveOccurred())

				// We want to label a node that is uncordoned and untainted,
				// so the pods can be scheduled
				for _, nodeDetails := range nodeList.Items {
					if (nodeDetails.Spec.Unschedulable != true) &&
						(len(nodeDetails.Spec.Taints) == 0) {
						nodeName = nodeDetails.ObjectMeta.Name
						break
					}
				}
				cmd := fmt.Sprintf("kubectl label node %v nodeselectortest=exists --overwrite", nodeName)
				_, _, err = utils.Run(cmd)
				Expect(err).ToNot(HaveOccurred())
			})

			// All the pods should be running on the labeled node
			By("confirm pods run on the labelled node", func() {
				AssertCreateCluster(namespace, clusterName, sampleFile, env)
				podList, err := env.GetPodList(namespace)
				Expect(err).ToNot(HaveOccurred())
				for _, podDetails := range podList.Items {
					if podDetails.Status.Phase == "Running" {
						Expect(podDetails.Spec.NodeName == nodeName).Should(Equal(true))
					}
				}
			})
		})
	})
})
