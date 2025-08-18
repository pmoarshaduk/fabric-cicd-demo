import yaml
from azure.identity import (
    InteractiveBrowserCredential,
    TokenCachePersistenceOptions
)
from fabric_cicd import FabricWorkspace, publish_all_items

# ── Option B: Disable persistent MSAL cache ──────────────────────────────────
cache_opts = TokenCachePersistenceOptions(enable_persistent_cache=False)

credential = InteractiveBrowserCredential(
    login_hint="254096@office365works.net",         # your target account
    prompt="select_account",                        # force account selector
    token_cache_persistence_options=cache_opts      # disable on-disk cache
)
# ──────────────────────────────────────────────────────────────────────────────

# Load and flatten your parameters
with open("parameter.yml", "r") as f:
    full_cfg = yaml.safe_load(f)

# If you’re still using a DEV wrapper
params = {}
for item_type, items in full_cfg.get("DEV", {}).items():
    for name, node in items.items():
        params[name] = node.get("parameters", {})

# Build your FabricWorkspace
workspace = FabricWorkspace(
    workspace_id="2d884eec-53e1-496a-b9bd-69afdd629ed9",
    repository_directory=".",
    credential=credential,
    item_type_in_scope=[
        "Notebook", "DataPipeline", "Environment", "SemanticModel", "Report"
    ],
    parameters=params
)

print("Starting interactive OAuth flow…")
publish_all_items(workspace)
print("Deployment complete.")
