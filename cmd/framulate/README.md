# Framulate

Runs a stress test on frames

## Usage example
```
kubectl delete job -n default-tenant framulate;
kubectl delete cm -n default-tenant framulate-config;
cat << EOF | kubectl apply -n default-tenant -f -
kind: ConfigMap 
apiVersion: v1 
metadata:
  name: framulate-config
data:
  framulate.yaml: |
    transport:
      # will use grpc by default. for http, use http://framesd:8080
      url: framesd:8081
    container_name: test10
    access_key: <your key>
    max_inflight_requests: 512
    cleanup: true
    scenario:
      kind: writeVerify
      writeVerify:
        verify: true
        num_tables: 32
        num_series_per_table: 10000
        max_parallel_tables_create: 32
        max_parallel_series_write: 512
        max_parallel_series_verify: 512
        write_dummy_series: true
        num_datapoints_per_series: 24
---
apiVersion: batch/v1
kind: Job
metadata:
  name: framulate
spec:
  template:
    spec:
      containers:
      - name: framulate
        image: iguazio/framulate:latest
        imagePullPolicy: Never
        args:
        - "-config-path"
        - "/etc/iguazio/framulate/framulate.yaml"
        volumeMounts:
        - name: config
          mountPath: /etc/iguazio/framulate
      restartPolicy: Never
      volumes:
      - name: config
        configMap:
          name: framulate-config
  backoffLimit: 0
EOF
```
