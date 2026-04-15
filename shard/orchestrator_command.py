import os

import requests

from shared.types.shard import ShardInfo


def _get_orchestrator_base_url() -> str:
    """Resolve the orchestrator base URL from env or default."""
    base = os.environ.get("ORCHESTRATOR_URL", "orchestrator")
    if not base.startswith("http"):
        base = f"http://{base}:3356"
    return base


class OrchestratorCommand:
    """Sends HTTP commands from a shard to the orchestrator."""

    def update_shard(self, shard_info: ShardInfo):
        """Register or heartbeat to the orchestrator."""
        url = f"{_get_orchestrator_base_url()}/shard"
        requests.post(url, json=shard_info.model_dump())
