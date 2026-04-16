import os

import httpx

from shared.types.shard import ShardInfo


def _get_orchestrator_base_url() -> str:
    """Resolve the orchestrator base URL from env or default."""
    url = os.environ.get("ORCHESTRATOR_URL", "http://orchestrator:3356")
    return url


class OrchestratorCommand:
    """Sends HTTP commands from a shard to the orchestrator."""

    def update_shard(self, shard_info: ShardInfo):
        """Register or heartbeat to the orchestrator."""
        url = f"{_get_orchestrator_base_url()}/shard"
        httpx.post(url, json=shard_info.model_dump())
