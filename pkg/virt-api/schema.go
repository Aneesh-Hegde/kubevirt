package virt_api

import (
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/runtime"
    "k8s.io/apimachinery/pkg/runtime/schema"
    "k8s.io/apimachinery/pkg/runtime/serializer"

    v1 "kubevirt.io/api/core/v1"
)

var (
    // Scheme holds the types for our API group
    Scheme = runtime.NewScheme()
    Codecs = serializer.NewCodecFactory(Scheme)
)

func init() {
    // register kubevirt types
    v1.AddToScheme(Scheme)

    // register meta types needed by the apiserver
    metav1.AddMetaToScheme(Scheme)

    // register our options types that don't have TypeMeta embedded
    // these are already registered via v1.AddToScheme via register.go
    // but we add the unversioned group here too
    Scheme.AddUnversionedTypes(
        schema.GroupVersion{Group: "", Version: "v1"},
        &metav1.Status{},
    )

kubevirtv1 := schema.GroupVersion{Group: "kubevirt.io", Version: "v1"}
    metav1.AddToGroupVersion(Scheme, kubevirtv1)

    Scheme.AddUnversionedTypes(
        schema.GroupVersion{Group: "", Version: "v1"},
        &metav1.Status{},
        &metav1.ListOptions{},
        &metav1.GetOptions{},
        &metav1.DeleteOptions{},
        &metav1.CreateOptions{},
        &metav1.UpdateOptions{},
        &metav1.PatchOptions{},
        &metav1.WatchEvent{},
    )

    Scheme.AddKnownTypes(kubevirtv1,
        &v1.StartOptions{},
        &v1.StopOptions{},
        &v1.RestartOptions{},
        &v1.PauseOptions{},
        &v1.UnpauseOptions{},
        &v1.MigrateOptions{},
    )
}
