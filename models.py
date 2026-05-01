from dataclasses import dataclass, field
from typing import List, Dict, Any

# Keep all existing models unchanged - Ollama compatible
@dataclass
class PodIssue:
    name: str
    namespace: str
    status: str
    reason: str
    message: str

@dataclass
class Diagnosis:
    pod_name: str
    root_cause: str
    severity: str
    action: str
    explanation: str
    fix_details: dict = field(default_factory=dict)
    namespace: str = "default"

@dataclass
class HealResult:
    pod_name: str
    success: bool
    action_taken: str
    details: str

@dataclass
class HealerInput:
    namespace: str = "default"
    auto_approve: bool = True

@dataclass
class ConversationInput:
    namespace: str = "default"
    session_id: str = ""
    messages: list = field(default_factory=list)
    healing_diagnoses: list = field(default_factory=list)
    healing_decisions: dict = field(default_factory=dict)
    turn_count: int = 0

# Updated for Ollama - renamed for clarity, backward compatible
@dataclass
class OllamaRequest:
    messages: List[Dict[str, str]]
    system_prompt: str
    model: str = "gemma2"

@dataclass
class OllamaResponse:
    stop_reason: str = "end_turn"  # Ollama default
    content: List[Dict[str, Any]]
