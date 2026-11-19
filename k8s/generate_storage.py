import argparse
import sys


def generate_node_yaml(node_name: str) -> str:
    # Extract shard id (digit after 's')
    # node_name format: s<shard>-<suffix>
    # e.g., s0-a, s1-b
    try:
        shard_part, suffix = node_name.split('-')
        shard_id = int(shard_part[1:])  # skip leading 's'
    except (TypeError, ValueError):
        raise ValueError(f"Invalid node name: {node_name}") from None
    role = "primary" if suffix == "a" else "replica"
    # Service name
    service_name = f"ems-{node_name}"
    # ADVERTISE_URL and PEER_URL patterns
    advertise_url = f"http://{service_name}-0.{service_name}.ems.svc.cluster.local:9000"
    # Determine peer (other node in same shard)
    peer_suffix = "b" if suffix == "a" else "a"
    peer_name = f"s{shard_id}-{peer_suffix}"
    peer_service = f"ems-{peer_name}"
    peer_url = f"http://{peer_service}-0.{peer_service}.ems.svc.cluster.local:9000"

    yaml_docs = []
    # Headless Service
    svc = f"""apiVersion: v1
kind: Service
metadata:
  name: {service_name}
  namespace: ems
  labels:
    app: {service_name}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  ports:
    - name: http
      port: 9000
      targetPort: 9000
  selector:
    app: {service_name}
"""
    yaml_docs.append(svc)
    # StatefulSet
    sts = f"""apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {service_name}
  namespace: ems
  labels:
    app: {service_name}
    component: storage-node
spec:
  serviceName: {service_name}
  replicas: 1
  selector:
    matchLabels:
      app: {service_name}
  template:
    metadata:
      labels:
        app: {service_name}
        component: storage-node
    spec:
      enableServiceLinks: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
      containers:
        - name: storage-node
          image: ems/storage-node:1.0
          imagePullPolicy: IfNotPresent
          env:
            - name: NODE_ID
              value: "{node_name}"
            - name: SHARD_ID
              value: "{shard_id}"
            - name: INITIAL_ROLE
              value: "{role}"
            - name: STANDALONE
              value: "false"
            - name: DATA_DIR
              value: "/data"
            - name: PORT
              value: "9000"
            - name: COORDINATOR_URL
              valueFrom:
                configMapKeyRef:
                  name: ems-config
                  key: COORDINATOR_URL
            - name: HEARTBEAT_INTERVAL_SEC
              valueFrom:
                configMapKeyRef:
                  name: ems-config
                  key: HEARTBEAT_INTERVAL_SEC
            - name: HTTP_TIMEOUT_SEC
              valueFrom:
                configMapKeyRef:
                  name: ems-config
                  key: HTTP_TIMEOUT_SEC
            - name: ADVERTISE_URL
              value: "{advertise_url}"
            - name: PEER_URL
              value: "{peer_url}"
          ports:
            - containerPort: 9000
          volumeMounts:
            - name: data
              mountPath: /data
          livenessProbe:
            httpGet:
              path: /health/live
              port: 9000
          readinessProbe:
            httpGet:
              path: /health/ready
              port: 9000
          startupProbe:
            httpGet:
              path: /health/live
              port: 9000
            failureThreshold: 30
            periodSeconds: 2
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 200Mi
"""
    yaml_docs.append(sts)
    # Join with document separator
    return "---\n".join(yaml_docs) + "\n"

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Generate storage node manifests")
    parser.add_argument(
        "--nodes",
        help="Comma‑separated node names (e.g., s0-a,s1-a,s2-a)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Generate manifests for all six nodes (a and b for each shard)",
    )
    args = parser.parse_args(argv)
    if args.all:
        node_list = [f"s{i}-{suffix}" for i in range(3) for suffix in ("a", "b")]
    elif args.nodes:
        node_list = [n.strip() for n in args.nodes.split(',') if n.strip()]
    else:
        # default three primary nodes
        node_list = [f"s{i}-a" for i in range(3)]

    for index, node in enumerate(node_list):
        if index:
            sys.stdout.write("---\n")
        sys.stdout.write(generate_node_yaml(node))

if __name__ == "__main__":
    main()
