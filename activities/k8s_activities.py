import re
import time
from typing import List

from kubernetes import client, config
from temporalio import activity

from models import PodIssue, Diagnosis, HealResult

def _init_k8s():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except config.ConfigException:
            raise RuntimeError("No Kubernetes cluster found. Run ./setup.sh or set KUBECONFIG.")
    return client.CoreV1Api(), client.AppsV1Api()

v1, apps_v1 = _init_k8s()

UNHEALTHY_REASONS = {
    "CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "OOMKilled",
    "CreateContainerConfigError", "RunContainerError", "InvalidImageName"
}

VALID_ACTIONS = {"restart_pod", "fix_image", "patch_resources", "skip"}

MEMORY_PATTERN = re.compile(r"^\d+[EPTGMK]i?$")
IMAGE_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_./:@-]+$")

@activity.defn
async def scan_cluster(namespace: str) -> List[PodIssue]:
    activity.logger.info(f"Scanning namespace '{namespace}'")
    pods = v1.list_namespaced_pod(namespace=namespace)
    issues = []
    # ... [full scan_cluster logic from previous response]
    # [Include ALL functions: scan_cluster, get_pod_details, _get_deployment_name, 
    #  _deployment_name_heuristic, _validate_fix, execute_fix]
    return issues

# ALL 6 functions must be complete - copy full version from earlier response
