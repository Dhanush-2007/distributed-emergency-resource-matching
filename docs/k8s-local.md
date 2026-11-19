# Local Kubernetes (Stage 3)

## Prerequisites
- Docker (Docker Desktop)
- Minikube
- kubectl
- Python 3.11 with project virtual environment (`.venv`)

## Commands

Start the walking skeleton:

```bash
scripts/k8s_up.sh
kubectl -n ems get pods,svc,pvc
kubectl -n ems port-forward svc/coordinator 8000:8000
```

Alternative access using NodePort:
```bash
minikube service coordinator-nodeport -n ems --url
```

In a second terminal, verify deployment:

```bash
.venv/bin/python scripts/k8s_verify_stage3.py
```

### Viewing Logs
To stream logs from coordinator or storage node pods:
```bash
kubectl -n ems logs -f deployment/coordinator
kubectl -n ems logs -f sts/ems-s1-a
```

### Teardown
Tear down the stack:
```bash
scripts/k8s_down.sh
```
To retain PVC data across restarts:
```bash
scripts/k8s_down.sh --keep-data
```

## Troubleshooting
- **ErrImageNeverPull / ImagePullBackOff**: Images are not loaded into Minikube. Run `make images` and then load them into Minikube:
  ```bash
  minikube image load ems/storage-node:1.0
  minikube image load ems/coordinator:1.0
  ```
- **Pending Pods**: Insufficient memory or CPU allocated to Minikube. Restart Minikube with at least 4 GB RAM: `minikube start --memory=4096 --cpus=2`.
- **Port-forward drops**: `kubectl port-forward` drops connection when the coordinator pod restarts or recovers. Restart the port-forward command.
- **PVC permission denied**: Non-root container UID/GID must match volume permissions. Pods use `securityContext.runAsUser: 10001` and `fsGroup: 10001`.
- **DNS names not resolving**: Storage node headless services need `publishNotReadyAddresses: true` so DNS names resolve while a node is still starting or catching up.
