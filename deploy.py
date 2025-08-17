import os
import shutil
import json
from pathlib import Path
from azure.identity.broker import InteractiveBrowserBrokerCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

def clear_token_cache():
    cache_path = Path(os.getenv("LOCALAPPDATA", "")) / ".IdentityService"
    if cache_path.exists():
        print(f"🧹 Clearing token cache at {cache_path}")
        try:
            shutil.rmtree(cache_path)
            print("✅ Token cache cleared.")
        except Exception as e:
            print(f"❌ Failed to clear token cache: {e}")
    else:
        print("ℹ️ No token cache found to clear.")

# 🔐 Step 1: Clear cache and login
clear_token_cache()
credential = InteractiveBrowserBrokerCredential()
