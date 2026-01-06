#!/bin/bash
set -e

# Record start time
START_TIME=$(date +%s)

echo "Starting Airflow infrastructure setup on Minikube..."

# --- Configuration & State ---
MINIKUBE_DRIVER="docker"
AIRFLOW_IMAGE_NAME="webshop-airflow-custom:latest"
JAVA_IMAGE_NAME="product-ranking-batch:latest"
JAVA_DOCKERFILE_PATH="pipelines/java/product-ranking-batch/Dockerfile"
NAMESPACE="default"
LOCAL_DAGS_PATH="dags/"
POD_DAGS_PATH="/opt/airflow/dags/"
DNS_NAMESERVER="8.8.8.8"

# Directory to store state files (like last build timestamps)
STATE_DIR="infra/.state"
mkdir -p "$STATE_DIR"

AIRFLOW_BUILD_MARKER="$STATE_DIR/last_build_airflow"
JAVA_BUILD_MARKER="$STATE_DIR/last_build_java"
DAG_SYNC_MARKER="$STATE_DIR/last_dag_sync"

# --- Pre-requisite Checks ---
echo "Checking for required tools: minikube and kubectl..."
if ! command -v minikube &> /dev/null; then
    echo "Error: minikube is not installed. Please install it to proceed."
    exit 1
fi
if ! command -v kubectl &> /dev/null; then
    echo "Error: kubectl is not installed. Please install it to proceed."
    exit 1
fi
echo "minikube and kubectl found."

# --- 1. Minikube Management ---
if ! minikube status &> /dev/null; then
  echo "Minikube not running. Starting cluster with driver $MINIKUBE_DRIVER..."
  minikube start --driver="$MINIKUBE_DRIVER"

  # If minikube was started (or restarted after delete), clean up local state markers
  echo "Minikube was (re)started. Cleaning up local build/sync markers to force a fresh deployment..."
  rm -f "$AIRFLOW_BUILD_MARKER" "$JAVA_BUILD_MARKER" "$DAG_SYNC_MARKER"
else
  echo "Minikube is already running."
fi

echo "Configuring Docker environment for Minikube..."
eval $(minikube -p minikube docker-env)

# --- Pre-pull common Kubernetes images to speed up first deploy ---
echo "Pre-pulling common Kubernetes images..."
minikube image pull postgres:16 || true
minikube image pull busybox:latest || true
echo "Finished pre-pulling common Kubernetes images."

# --- 2. Apply DNS fix in Minikube VM ---
echo "Applying DNS fix in Minikube VM (to prevent 'server misbehaving')..."
minikube ssh "echo \"nameserver $DNS_NAMESERVER\" | sudo tee /etc/resolv.conf > /dev/null"
echo "Restarting Docker inside Minikube to apply DNS changes..."
minikube ssh "sudo systemctl restart docker"
sleep 5

# --- 3. Build Custom Docker Images inside Minikube (Smartly) ---
# Check if Airflow image needs rebuild by checking for its existence in minikube and file changes
AIRFLOW_IMAGE_EXISTS=$(minikube image ls | grep "$AIRFLOW_IMAGE_NAME" || true)
if [ ! -f "$AIRFLOW_BUILD_MARKER" ] || [ "Dockerfile" -nt "$AIRFLOW_BUILD_MARKER" ] || [ "requirements.txt" -nt "$AIRFLOW_BUILD_MARKER" ] || [ -z "$AIRFLOW_IMAGE_EXISTS" ]; then
  echo "Building '$AIRFLOW_IMAGE_NAME' as it's missing or source files have changed..."
  minikube image build -t "$AIRFLOW_IMAGE_NAME" .
  touch "$AIRFLOW_BUILD_MARKER"
else
  echo "Airflow image is up-to-date. Skipping build."
fi

# Check if Java image needs rebuild by checking for its existence in minikube and file changes
JAVA_IMAGE_EXISTS=$(minikube image ls | grep "$JAVA_IMAGE_NAME" || true)
if [ ! -f "$JAVA_BUILD_MARKER" ] || [ -n "$(find pipelines/java/product-ranking-batch -type f -newer "$JAVA_BUILD_MARKER")" ] || [ -z "$JAVA_IMAGE_EXISTS" ]; then
  echo "Building '$JAVA_IMAGE_NAME' as it's missing or source files have changed..."
  minikube image build -t "$JAVA_IMAGE_NAME" -f "$JAVA_DOCKERFILE_PATH" .
  touch "$JAVA_BUILD_MARKER"
else
  echo "Java image is up-to-date. Skipping build."
fi

# --- 4. Load Environment and Create Secrets ---
echo "Loading secrets from .env file..."
if [ ! -f .env ]; then
    echo "Error: .env file not found. Please copy .env.example to .env and fill in your credentials."
    exit 1
fi

export $(grep -v '^#' .env | xargs)

if [ -z "$WEB_SHOP_DB_PASSWORD" ] || [ -z "$ANALYTICS_DB_PASSWORD" ]; then
    echo "Error: Database passwords are not set in the .env file."
    exit 1
fi

echo "Creating Kubernetes Secrets from environment variables..."
kubectl delete secret app-postgres-secret --ignore-not-found
kubectl delete secret analytics-db-secret --ignore-not-found
kubectl delete secret ranking-batch-secret --ignore-not-found

kubectl create secret generic app-postgres-secret \
  --from-literal=user="$WEB_SHOP_DB_USER" \
  --from-literal=password="$WEB_SHOP_DB_PASSWORD"
kubectl create secret generic analytics-db-secret \
  --from-literal=user="$ANALYTICS_DB_USER" \
  --from-literal=password="$ANALYTICS_DB_PASSWORD"
kubectl create secret generic ranking-batch-secret \
  --from-literal=DB_USER_APP="$WEB_SHOP_DB_USER" \
  --from-literal=DB_PASSWORD_APP="$WEB_SHOP_DB_PASSWORD" \
  --from-literal=DB_USER_ANALYTICS="$ANALYTICS_DB_USER" \
  --from-literal=DB_PASSWORD_ANALYTICS="$ANALYTICS_DB_PASSWORD"

echo "Creating app-db-init-scripts ConfigMap dynamically from webshop/webshop_sql..."
kubectl delete configmap app-db-init-scripts -n "$NAMESPACE" --ignore-not-found || true
kubectl create configmap app-db-init-scripts --from-file=webshop/webshop_sql -n "$NAMESPACE"

echo "Creating analytics-fdw-scripts ConfigMap dynamically from webshop_analytics/..."
kubectl delete configmap analytics-fdw-scripts -n "$NAMESPACE" --ignore-not-found || true
kubectl create configmap analytics-fdw-scripts --from-file=webshop_analytics -n "$NAMESPACE"

# --- 5. Apply Kustomization (Deployments and Services) ---
echo "Deleting existing jobs to ensure re-initialization..."
kubectl delete job airflow-db-init-job -n "$NAMESPACE" --ignore-not-found || true
kubectl delete job analytics-db-fdw-init-job -n "$NAMESPACE" --ignore-not-found || true
kubectl delete job app-db-init-job -n "$NAMESPACE" --ignore-not-found || true

echo "Applying Kubernetes infrastructure configurations via kustomization..."
kubectl apply -k infra/

# --- 6. Wait for Critical Components (In Parallel) ---
echo "Waiting for all database pods to be ready..."
kubectl wait --for=condition=ready pod -l app=airflow-postgres -n "$NAMESPACE" --timeout=600s &
PID1=$!
kubectl wait --for=condition=ready pod -l app=app-postgres -n "$NAMESPACE" --timeout=600s &
PID2=$!
kubectl wait --for=condition=ready pod -l app=analytics-db -n "$NAMESPACE" --timeout=600s &
PID3=$!
wait $PID1 $PID2 $PID3
echo "All database pods are ready."

echo "Waiting for all database init jobs to complete..."
kubectl wait --for=condition=complete job/airflow-db-init-job -n "$NAMESPACE" --timeout=300s &
PID1=$!
kubectl wait --for=condition=complete job/app-db-init-job -n "$NAMESPACE" --timeout=300s &
PID2=$!
kubectl wait --for=condition=complete job/analytics-db-fdw-init-job -n "$NAMESPACE" --timeout=300s &
PID3=$!
wait $PID1 $PID2 $PID3
echo "All database init jobs completed."

echo "Waiting for all Airflow deployments to be available..."
kubectl wait --for=condition=available deployment/airflow-apiserver -n "$NAMESPACE" --timeout=300s &
PID1=$!
kubectl wait --for=condition=available deployment/airflow-scheduler -n "$NAMESPACE" --timeout=300s &
PID2=$!
kubectl wait --for=condition=available deployment/airflow-dag-processor -n "$NAMESPACE" --timeout=300s &
PID3=$!
wait $PID1 $PID2 $PID3
echo "All Airflow deployments are available."

# --- 7. Synchronize DAGs (Smartly) ---
if [ ! -f "$DAG_SYNC_MARKER" ] || [ -n "$(find dags/ pipelines/ -type f -newer "$DAG_SYNC_MARKER")" ]; then
  echo "Changes detected in dags/ or pipelines/. Synchronizing files..."
  AIRFLOW_POD_NAME=$(kubectl get pods -n "$NAMESPACE" -l app=airflow-apiserver -o jsonpath='{.items[0].metadata.name}')
  if [ -z "$AIRFLOW_POD_NAME" ]; then
    echo "Error: Could not find 'airflow-apiserver' pod to synchronize DAGs."
    exit 1
  fi
  kubectl cp "$LOCAL_DAGS_PATH/." "$NAMESPACE/$AIRFLOW_POD_NAME:$POD_DAGS_PATH"
  kubectl cp "pipelines/." "$NAMESPACE/$AIRFLOW_POD_NAME:$POD_DAGS_PATH/pipelines"
  echo "DAGs and custom modules copied."
  touch "$DAG_SYNC_MARKER"
else
  echo "DAGs and pipelines are up-to-date. Skipping synchronization."
fi

# --- 8. Airflow UI Access Information ---
echo ""
echo "--------------------------------------------------------"
echo "Airflow infrastructure successfully deployed on Minikube!"
echo "--------------------------------------------------------"
echo ""

MINIKUBE_IP=$(minikube ip)
AIRFLOW_NODEPORT=$(kubectl get service airflow-apiserver -n "$NAMESPACE" -o jsonpath='{.spec.ports[0].nodePort}')

echo "To access the Airflow UI, open your browser at:"
echo "  http://$MINIKUBE_IP:$AIRFLOW_NODEPORT"
echo ""
echo "Note: Builds and DAG syncs are now automatic based on file changes."
echo ""

# Record end time and calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
echo "Total deployment time: ${DURATION} seconds."
