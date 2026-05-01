import argparse
import asyncio
import time

from dotenv import load_dotenv
from temporalio.client import Client

load_dotenv()

from models import HealerInput
from workflows.healer_workflow import HealerWorkflow

async def main():
    parser = argparse.ArgumentParser(description="Start KubeHealer (Gemma2 auto-healing)")
    parser.add_argument("--namespace", default="default", help="Kubernetes namespace to scan")
    parser.add_argument("--manual", action="store_true", help="Manual approval mode (send signals)")
    args = parser.parse_args()

    client = await Client.connect("localhost:7233")

    workflow_id = f"kubehealer-{int(time.time())}"
    print(f"🚀 Starting KubeHealer (Gemma2-powered) (id={workflow_id})")
    print(f"   Namespace: {args.namespace}")
    print(f"   Mode: {'Manual' if args.manual else 'Auto-approve'}")
    print()

    # Auto-approve by default, manual waits for signals
    healer_input = HealerInput(
        namespace=args.namespace, 
        auto_approve=not args.manual
    )

    handle = await client.start_workflow(
        HealerWorkflow.run,
        healer_input,
        id=workflow_id,
        task_queue="kubehealer",
    )

    print(f"📊 Live trace: http://localhost:8233/namespaces/default/workflows/{workflow_id}")
    
    if args.manual:
        print("\n🔄 Manual mode - approve/reject pods:")
        print("   temporal workflow signal HealerWorkflow approve_pod PODNAME --workflow-id {workflow_id}")
        print("   temporal workflow signal HealerWorkflow reject_pod PODNAME --workflow-id {workflow_id}")
        print("\n⏳ Waiting for completion...")
        result = await handle.result()
    else:
        print("⏳ Auto-healing...")
        result = await handle.result()

    print("\n✅ Healing complete:")
    print(result)
    print(f"\n📊 Full trace: http://localhost:8233/namespaces/default/workflows/{workflow_id}")

if __name__ == "__main__":
    asyncio.run(main())
