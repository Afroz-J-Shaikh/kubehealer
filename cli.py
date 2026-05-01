import asyncio
import sys
import time
from dotenv import load_dotenv
from temporalio.client import Client
from temporalio.service import RPCError

load_dotenv()

from models import ConversationInput
from workflows.conversation_workflow import ConversationWorkflow

WORKFLOW_ID = f"kubehealer-conversation-{int(time.time())}"

BANNER = """
\033[1;96m  ╔═══════════════════════════════════════╗
  ║         KubeHealer AI Assistant        ║
  ╚═══════════════════════════════════════╝\033[0m
  \033[2mAI-powered Kubernetes debugging with Ollama Gemma2 + Temporal.\033[0m
"""

async def terminate_stale_workflow(client: Client):
    """Terminate a stale workflow from a previous code version."""
    try:
        handle = client.get_workflow_handle(WORKFLOW_ID)
        await handle.terminate(reason="Restarting with Ollama Gemma2")
        print("  \033[2m[terminated stale session]\033[0m")
        await asyncio.sleep(1)
    except RPCError:
        pass

async def get_or_start_workflow(client: Client, namespace: str):
    """Reconnect to an existing conversation or start a new one."""
    try:
        handle = client.get_workflow_handle(WORKFLOW_ID)
        await handle.query(ConversationWorkflow.get_state)
        return handle, False
    except RPCError:
        handle = await client.start_workflow(
            ConversationWorkflow.run,
            ConversationInput(namespace=namespace, session_id=WORKFLOW_ID),
            id=WORKFLOW_ID,
            task_queue="kubehealer",
        )
        return handle, True

async def main():
    client = await Client.connect("localhost:7233")
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"

    handle, is_new = await get_or_start_workflow(client, namespace)

    print(BANNER)
    print(f"  Session: {WORKFLOW_ID} | Namespace: {namespace} | Model: Gemma2")
    if is_new:
        print("  Talk to me about your cluster. Try: 'list pods', 'scan', 'heal'!")
    else:
        print("  (reconnected)")
        state = await handle.query(ConversationWorkflow.get_state)
        if state["latest_response"]:
            print(f"\n  \033[2m[Last response]:\033[0m\n{state['latest_response']}")
    print()

    while True:
        try:
            user_input = input("\033[96myou>\033[0m ")
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye!")
            break

        if not user_input.strip():
            continue

        if user_input.strip().lower() in ("exit", "quit", "bye"):
            try:
                await handle.execute_update(
                    ConversationWorkflow.send_message, "exit"
                )
            except Exception:
                pass
            print("  Goodbye!")
            break

        print(f"  \033[2m[Gemma2 thinking...]\033[0m", end="\r", flush=True)

        try:
            response = await handle.execute_update(
                ConversationWorkflow.send_message, user_input
            )
            print(f"\033[2K\n{response}\n")
        except RPCError as e:
            error_msg = str(e)
            if "workflow execution already completed" in error_msg.lower():
                print(f"\033[2K  \033[93mSession expired. Starting fresh...\033[0m")
                handle, _ = await get_or_start_workflow(client, namespace)
                continue
            print(f"\033[2K  \033[91mError: {e}\033[0m\n")
        except Exception as e:
            error_msg = str(e)
            if "nondeterminism" in error_msg.lower():
                print(f"\033[2K  \033[93mStale session. Restarting...\033[0m")
                await terminate_stale_workflow(client)
                handle, _ = await get_or_start_workflow(client, namespace)
                try:
                    response = await handle.execute_update(
                        ConversationWorkflow.send_message, user_input
                    )
                    print(f"\033[2K\n{response}\n")
                except Exception as retry_err:
                    print(f"\033[2K  \033[91mError: {retry_err}\033[0m\n")
            else:
                print(f"\033[2K  \033[91mError: {e}\033[0m\n")

if __name__ == "__main__":
    asyncio.run(main())
