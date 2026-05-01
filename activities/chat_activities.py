import ollama  # Replaced anthropic
from kubernetes import client, config
from temporalio import activity
from temporalio.exceptions import ApplicationError
from models import OllamaRequest, OllamaResponse  # Updated import

def _init_k8s():
    """Initialize Kubernetes client. Tries in-cluster first, falls back to kubeconfig."""
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except config.ConfigException:
            raise RuntimeError(
                "No Kubernetes cluster found. "
                "Run ./setup.sh first, or set KUBECONFIG."
            )
    return client.CoreV1Api()

v1 = _init_k8s()

# ── Ollama Gemma2 API activity ───────────────────────────────────────
@activity.defn
async def call_ollama(request: OllamaRequest) -> OllamaResponse:
    """Call Ollama Gemma2. Returns serializable response."""
    activity.logger.info(f"Calling Ollama Gemma2 ({len(request.messages)} messages)")

    try:
        # Build messages array for Ollama
        ollama_messages = []
        for msg in request.messages:
            role = "user" if msg.get("role") == "user" else "assistant"
            ollama_messages.append({"role": role, "content": msg.get("content", "")})
        
        # System prompt first
        full_messages = [{"role": "system", "content": request.system_prompt}] + ollama_messages

        response = ollama.chat(
            model=request.model,
            messages=full_messages,
            options={
                "temperature": 0.1,  # Low for Kube accuracy
                "num_predict": 2048  # Max tokens
            }
        )
        
        # Ollama response format: {'message': {'role': 'assistant', 'content': '...', 'tool_calls': []}}
        content_text = response['message']['content']
        
        # Convert to Claude-like content blocks for compatibility
        content_dicts = [
            {
                "type": "text",
                "text": content_text
            }
        ]
        
        # Ollama doesn't have stop_reason - use default
        stop_reason = "end_turn"
        
        activity.logger.info(f"Response: stop_reason={stop_reason}, {len(content_dicts)} blocks")
        return OllamaResponse(stop_reason=stop_reason, content=content_dicts)
        
    except Exception as e:
        activity.logger.error(f"Ollama error: {e}")
        raise ApplicationError(f"Ollama Gemma2 failed: {str(e)}")

# ── Kubernetes read activities ───────────────────────────────── (UNCHANGED)
@activity.defn
async def list_pods_activity(namespace: str) -> str:
    """List all pods in a namespace with status, readiness, and restart count."""
    activity.logger.info(f"Listing pods in namespace '{namespace}'")
    pods = v1.list_namespaced_pod(namespace=namespace)

    lines = [f"{'NAME':<50} {'STATUS':<25} {'READY':<8} {'RESTARTS'}"]
    lines.append("-" * 95)

    for pod in pods.items:
        name = pod.metadata.name
        phase = pod.status.phase or "Unknown"
        ready = "0/0"
        restarts = 0

        if pod.status.container_statuses:
            total = len(pod.status.container_statuses)
            ready_count = sum(1 for cs in pod.status.container_statuses if cs.ready)
            ready = f"{ready_count}/{total}"
            restarts = sum(cs.restart_count for cs in pod.status.container_statuses)

            for cs in pod.status.container_statuses:
                if cs.state and cs.state.waiting and cs.state.waiting.reason:
                    phase = cs.state.waiting.reason
                    break
                if cs.state and cs.state.terminated and cs.state.terminated.reason:
                    phase = cs.state.terminated.reason
                    break

        lines.append(f"{name:<50} {phase:<25} {ready:<8} {restarts}")

    return "\n".join(lines)  # Fixed double \\

@activity.defn
async def get_pod_details_activity(pod_name: str, namespace: str) -> str:
    """Get detailed info about a specific pod."""
    activity.logger.info(f"Getting details for pod '{pod_name}'")
    pod = v1.read_namespaced_pod(name=pod_name, namespace=namespace)

    lines = [f"Pod: {pod_name}", f"Namespace: {namespace}", f"Phase: {pod.status.phase}"]

    if pod.status.container_statuses:
        for cs in pod.status.container_statuses:
            lines.append(f"\nContainer: {cs.name}")
            lines.append(f"  Image: {cs.image}")
            lines.append(f"  Ready: {cs.ready}")
            lines.append(f"  Restart Count: {cs.restart_count}")
            if cs.state:
                if cs.state.waiting:
                    lines.append(f"  State: Waiting — {cs.state.waiting.reason}: {cs.state.waiting.message}")
                elif cs.state.terminated:
                    lines.append(f"  State: Terminated — {cs.state.terminated.reason}")
                elif cs.state.running:
                    lines.append("  State: Running")

    if pod.status.conditions:
        lines.append("\nConditions:")
        for cond in pod.status.conditions:
            lines.append(f"  {cond.type}: {cond.status} — {cond.message or ''}")

    for container in pod.spec.containers:
        if container.resources and container.resources.limits:
            lines.append(f"\nResource Limits ({container.name}):")
            for k, val in container.resources.limits.items():
                lines.append(f"  {k}: {val}")

    return "\n".join(lines)

@activity.defn
async def get_pod_logs_activity(pod_name: str, namespace: str, tail_lines: int) -> str:
    """Get recent log output from a pod."""
    activity.logger.info(f"Getting logs for pod '{pod_name}' (tail={tail_lines})")
    try:
        logs = v1.read_namespaced_pod_log(name=pod_name, namespace=namespace, tail_lines=tail_lines)
        return logs if logs else "(no log output)"
    except Exception as e:
        return f"Could not get logs: {e}"

@activity.defn
async def get_pod_events_activity(pod_name: str, namespace: str) -> str:
    """Get Kubernetes events for a pod."""
    activity.logger.info(f"Getting events for pod '{pod_name}'")
    events = v1.list_namespaced_event(
        namespace=namespace,
        field_selector=f"involvedObject.name={pod_name},involvedObject.kind=Pod",
    )
    if not events.items:
        return f"No events found for pod '{pod_name}'."

    lines = []
    for event in events.items[-15:]:
        lines.append(f"[{event.type:<8}] {event.reason:<25} {event.message}")
    return "\n".join(lines)
