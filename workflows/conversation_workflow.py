from datetime import timedelta
from typing import List, Dict, Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities.chat_activities import (
        call_ollama,  # Changed from call_claude
        list_pods_activity,
        get_pod_details_activity,
        get_pod_logs_activity,
        get_pod_events_activity,
    )
    from activities.k8s_activities import scan_cluster, get_pod_details, execute_fix
    from activities.llm_activities import diagnose_pod
    from models import OllamaRequest, OllamaResponse, ConversationInput, Diagnosis  # Updated imports

# ── NO TOOLS - Gemma2 uses conversation only ────────────────────
# Tools disabled per request - Gemma2 analyzes from pod details text

SIMPLE_SYSTEM_PROMPT = """You are KubeHealer, a Kubernetes debugging assistant. 

You receive pod details and cluster info as text input. Analyze issues and respond conversationally.

When asked about specific pods, describe their status, suggest fixes, or recommend commands like:
- kubectl delete pod <name> -n <namespace> 
- kubectl rollout restart deployment <name> -n <namespace>

For healing, say "start scan" and I'll provide cluster data. No function calls needed.

Keep responses short and terminal-friendly."""

MAX_TURNS = 50

@workflow.defn
class ConversationWorkflow:
    def __init__(self):
        self._namespace: str = "default"
        self._session_id: str = ""
        self._messages: List[Dict[str, str]] = []
        self._latest_response: str = ""
        self._waiting_for_input: bool = True
        self._processing: bool = False
        self._turn_count: int = 0
        self._done: bool = False
        self._needs_continue_as_new: bool = False
        # Healing state
        self._healing_diagnoses: List[Dict] = []
        self._healing_decisions: Dict[str, str] = {}
        self._healing_pending: List[str] = []

    @workflow.update
    async def send_message(self, text: str) -> str:
        if text.strip().lower() in ("exit", "quit", "bye"):
            self._done = True
            return "Goodbye!"
    
        self._messages.append({"role": "user", "content": text})
        self._turn_count += 1
    
        # COMMAND ROUTER FIRST
        cmd_result = await self._handle_user_command(text)
        if cmd_result:
            self._messages.append({"role": "assistant", "content": cmd_result})
            self._latest_response = cmd_result
            return cmd_result
    
        # FALLBACK to chat
        await self._simple_ollama_chat(self._namespace)
        return self._latest_response

    @send_message.validator
    def validate_send_message(self, text: str) -> None:
        if not text or not text.strip():
            raise ValueError("Message cannot be empty")
        if self._processing:
            raise ValueError("Already processing a message, please wait")

    @workflow.query
    def get_state(self) -> dict:
        return {
            "latest_response": self._latest_response,
            "waiting_for_input": self._waiting_for_input,
            "processing": self._processing,
            "turn_count": self._turn_count,
            "messages_count": len(self._messages),
            "healing_pending": list(self._healing_pending),
        }

    @workflow.query
    def get_messages(self) -> List[Dict]:
        return list(self._messages)

    @workflow.run
    async def run(self, input: ConversationInput) -> str:
        self._namespace = input.namespace
        self._session_id = input.session_id

        if input.messages:
            self._messages = list(input.messages)
        if input.healing_diagnoses:
            self._healing_diagnoses = list(input.healing_diagnoses)
        if input.healing_decisions:
            self._healing_decisions = dict(input.healing_decisions)
            decided = set(input.healing_decisions.keys())
            all_pods = {d["pod_name"] for d in input.healing_diagnoses}
            self._healing_pending = list(all_pods - decided)
        self._turn_count = input.turn_count

        await workflow.wait_condition(
            lambda: self._done or self._needs_continue_as_new
        )

        if self._needs_continue_as_new:
            trimmed = self._messages[-40:] if len(self._messages) > 40 else list(self._messages)
            workflow.continue_as_new(
                ConversationInput(
                    namespace=self._namespace,
                    session_id=self._session_id,
                    messages=trimmed,
                    healing_diagnoses=self._healing_diagnoses,
                    healing_decisions=self._healing_decisions,
                    turn_count=0,
                )
            )
        return "Conversation ended."

    # ── Simple Ollama chat (NO tools) ──────────────────────────
    async def _simple_ollama_chat(self, namespace: str) -> None:
        # Build context from recent messages + cluster state
        context = f"Namespace: {namespace}\n"
        if self._healing_pending:
            context += f"Healing pending: {', '.join(self._healing_pending)}\n"
        
        recent_messages = self._messages[-10:]  # Last 10 messages
        request = OllamaRequest(
            messages=recent_messages,
            system_prompt=SIMPLE_SYSTEM_PROMPT + "\n" + context,
            model="gemma4"
        )

        response: OllamaResponse = await workflow.execute_activity(
            call_ollama,
            request,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # Store and display response
        self._messages.append({"role": "assistant", "content": response.content[0]["text"]})
        self._latest_response = response.content[0]["text"]

    # ── Manual command handling (user types commands) ──────────
    async def _handle_user_command(self, text: str) -> str:
        text_lower = text.strip().lower()
        namespace = self._namespace

        if "list pods" in text_lower or "show pods" in text_lower:
            return await workflow.execute_activity(
                list_pods_activity, namespace,
                start_to_close_timeout=timedelta(seconds=30),
            )

        elif "scan" in text_lower or "heal" in text_lower:
            return await self._handle_start_healing(namespace)

        elif "approve" in text_lower and self._healing_pending:
            # Extract pod name from text
            words = text_lower.split()
            if "all" in words:
                for pod in self._healing_pending[:]:
                    self._healing_decisions[pod] = "approved"
                    self._healing_pending.remove(pod)
                return "Approved all fixes. Executing..." if not self._healing_pending else "Approved all."
            else:
                for pod in self._healing_pending[:]:
                    if pod.lower() in text_lower:
                        self._healing_decisions[pod] = "approved"
                        self._healing_pending.remove(pod)
                        return f"Approved {pod}." if self._healing_pending else await self._execute_all_fixes()

        elif "reject" in text_lower and self._healing_pending:
            for pod in self._healing_pending[:]:
                if pod.lower() in text_lower or "all" in text_lower.split():
                    self._healing_decisions[pod] = "rejected"
                    self._healing_pending.remove(pod)
            return "Rejected." if self._healing_pending else await self._execute_all_fixes()

        return None

    # ── Healing methods (unchanged from original) ──────────────
    async def _handle_start_healing(self, namespace: str) -> str:
        if self._healing_pending:
            pending = ", ".join(sorted(self._healing_pending))
            return f"Healing already active with pending decisions for: {pending}\nUse 'approve <pod>' or 'reject <pod>'."

        self._healing_diagnoses = []
        self._healing_decisions = {}
        self._healing_pending = []

        issues = await workflow.execute_activity(
            scan_cluster, namespace,
            start_to_close_timeout=timedelta(seconds=30),
        )

        if not issues:
            return "All pods are healthy! Nothing to fix."

        for issue in issues:
            details = await workflow.execute_activity(
                get_pod_details_activity,
                issue.name, issue.namespace,  # Positional args!
                start_to_close_timeout=timedelta(seconds=30),
            )

            diagnosis: Diagnosis = await workflow.execute_activity(
                diagnose_pod, 
                details,  # String from get_pod_details
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )

            diag_dict = diagnosis.__dict__
            self._healing_diagnoses.append(diag_dict)
            self._healing_pending.append(diagnosis.pod_name)

        lines = [f"Found {len(self._healing_diagnoses)} issue(s):\n"]
        for i, d in enumerate(self._healing_diagnoses, 1):
            lines.append(f"  {i}. {d['pod_name']}")
            lines.append(f"     Severity: {d['severity'].upper()}")
            lines.append(f"     Root Cause: {d['root_cause']}")
            lines.append(f"     Action: {d['action']}")
            lines.append(f"     Explanation: {d['explanation']}")
            if d["fix_details"]:
                lines.append(f"     Fix Details: {d['fix_details']}")
            lines.append("")

        lines.append(f"Reply 'approve <podname>' or 'reject <podname>' or 'approve all'")
        return "\n".join(lines)

    async def _execute_all_fixes(self) -> str:
        lines = ["All decisions made. Executing fixes...\n"]

        for diag_dict in self._healing_diagnoses:
            decision = self._healing_decisions.get(diag_dict["pod_name"], "rejected")

            if decision == "approved" and diag_dict["action"] != "skip":
                diagnosis = Diagnosis(**diag_dict)
                result = await workflow.execute_activity(
                    execute_fix, diagnosis,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                icon = "OK" if result.success else "--"
                lines.append(f"  [{icon}] {result.pod_name}: {result.action_taken} — {result.details}")
            else:
                reason = diag_dict["explanation"] if diag_dict["action"] == "skip" else "Rejected by user"
                lines.append(f"  [--] {diag_dict['pod_name']}: skipped — {reason}")

        self._healing_diagnoses = []
        self._healing_decisions = {}
        self._healing_pending = []
        return "\n".join(lines)
