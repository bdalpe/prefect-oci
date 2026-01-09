# Prefect OCI Artifact Tools

⚠️ **This project is in experimental status and not ready for production usage.** ⚠️

This repository contains tools for working with OCI (Open Container Initiative) artifacts. It also provides the ability to push and pull artifacts to and from OCI-compliant registries without requiring a container runtime like Docker to be present. 

These images are a lightweight alternative to using the full `prefect_docker.deployment.steps.build_docker_image` command, allowing for faster builds and smaller image sizes.

## Why?

Prefect’s current container-based deployment model tightly couples workflow code, workflow dependencies, and the Prefect runtime. In practice, this means that upgrading Prefect or making small dependency changes often requires rebuilding and redeploying large images, even when most of the environment hasn’t changed. The official Prefect base images are also quite large (often 750 MB+ decompressed), which leads to slow builds, longer push/pull times, and higher infrastructure costs—problems that compound quickly in enterprise environments.

This project exists to make Prefect workflow packaging faster, smaller, and more scalable. By building workflows directly into OCI-compatible images/artifacts using native Python tooling, it removes the need for Docker or other third-party binaries at build time. This makes builds more portable, easier to run in constrained CI environments, and simpler to reason about as a Python-first process. It also unlocks the ability to use the OCI image as a Kubernetes Volume, which can be a simpler and more efficient way to deliver workflow code to Prefect workers running in Kubernetes clusters.

A core goal is reproducibility. Identical inputs—such as the same requirements.txt—should always produce identical layers and hashes. Traditional container builds often fail here due to timestamp and filesystem metadata changes. This library creates deterministic, cache-friendly layers, enabling faster builds and better reuse across deployments.

Ultimately, this approach decouples workflow code from the Prefect runtime, produces significantly smaller images, and enables faster, more reliable builds that scale better for teams and organizations running Prefect at scale.

## Features

Available steps for `prefect.yaml`:
* build:
    - prefect_oci.deployments.steps.install_dependencies_for_archiving
    - prefect_oci.deployments.steps.create_tar_archive
* push:
    - prefect_oci.deployments.steps.push_oci_image
* pull:
    - prefect_oci.deployments.steps.pull_oci_image

## Example Usage

A simple example to package code and dependencies into a single layer and push it to a registry:

```yaml
build:
  - prefect_oci.deployments.steps.install_dependencies_for_archiving:
      id: requirements
  - prefect_oci.deployments.steps.create_tar_archive:
      id: flow
      sources: ./src/my_flow
      
push:
  - prefect_oci.deployments.steps.push_oci_image:
      name: localhost:5002/my-flow
      tag: latest
      layers:
        - "{{ flow.output_path }}"
```
