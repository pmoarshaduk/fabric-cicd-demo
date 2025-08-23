"""
Fabric CI/CD Deployment Script (Device Code Flow)
Purpose: Deploy notebooks across DEV/TEST/PROD workspaces
Authentication: Uses device code flow with Microsoft public client
Works with: Python 3.12, Microsoft Fabric, Office 365 account
"""

import os
import json
import requests
from msal import PublicClientApplication
import logging
from typing import Dict, List

# ----------------------------
# CONFIGURATION
# ----------------------------
WORKSPACES: Dict[str, str] = {
    "dev": "2d884eec-53e1-496a-b9bd-69afdd529ed9",   # WS_DWH_DEV
    "test": "7bf01073-78b1-4c5d-86c8-629a0237243b",  # WS_DWH_TEST
    "prod": "69850df3-07c5-431b-ab85-475fe2266e70"   # WS_DWH_PROD
}

ITEMS_TO_DEPLOY: List[str] = [
    "notebooks/notebook_demo.ipynb"
]

CLIENT_ID: str = "d3590ed6-52b3-4102-aeff-aad2292ab01c"  # Microsoft public client
AUTHORITY: str = "https://login.microsoftonline.com/organizations"
SCOPES: List[str] = ["https://analysis.windows.net/powerbi/api/.default"]

# ----------------------------
# AUTHENTICATION
# ----------------------------
def authenticate() -> str:
    """Get OAuth token using device code flow"""
    app = PublicClientApplication(client_id=CLIENT_ID, authority=AUTHORITY)
    device_flow = app.initiate_device_flow(scopes=SCOPES)

    if "user_code" not in device_flow:
        raise ValueError("Failed to initiate device flow")

    print("\n==========================================")
    print("To authenticate, please:")
    print(f"1. Open: {device_flow['verification_uri']}")
    print(f"2. Enter code: {device_flow['user_code']}")
    print("==========================================\n")

    result = app.acquire_token_by_device_flow(device_flow)

    if "access_token" not in result:
        raise ValueError(f"Authentication failed: {result.get('error_description', 'Unknown error')}")

    print("Authentication successful!")
    return result["access_token"]

# ----------------------------
# DEPLOY NOTEBOOK AS WRAPPED JSON PAYLOAD
# ----------------------------
def deploy_notebook(token: str, workspace_id: str, notebook_path: str) -> None:
    """Deploy notebook to Fabric using wrapped JSON payload"""
    if not os.path.exists(notebook_path):
        raise FileNotFoundError(f"Notebook file not found: {notebook_path}")

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/notebooks"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        with open(notebook_path, "r", encoding="utf-8") as f:
            notebook_json = json.load(f)

        payload = {
            "name": os.path.splitext(os.path.basename(notebook_path))[0],
            "notebook": notebook_json
        }

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 201:
            logging.error(f"❌ API response: {response.status_code} - {response.text}")
            response.raise_for_status()

        logging.info(f"✅ Successfully deployed {os.path.basename(notebook_path)} to workspace {workspace_id}")
    except Exception as e:
        logging.error(f"❌ Failed to deploy notebook: {str(e)}")
        raise

# ----------------------------
# MANUAL TABLE DEPLOYMENT INSTRUCTIONS
# ----------------------------
def deploy_table(token: str, workspace_id: str) -> None:
    """Provide instructions for manual table deployment"""
    logging.warning(
        f"\n⚠️ Manual step required for table 'sales_data':\n"
        f"1. In Fabric portal, go to workspace '{workspace_id}'\n"
        f"2. Open a notebook attached to your lakehouse\n"
        f"3. Run this PySpark code:\n\n"
        f"df = spark.read.format('csv').option('header','true').load('Files/sales_data.csv')\n"
        f"df.write.format('delta').mode('overwrite').saveAsTable('sales_data')\n"
    )

# ----------------------------
# MAIN EXECUTION
# ----------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('fabric_deployment.log')
        ]
    )

    try:
        logging.info("🚀 Starting Fabric deployment...")
        token = authenticate()

        for env, workspace_id in WORKSPACES.items():
            logging.info(f"\n🔧 Deploying to {env.upper()} workspace (ID: {workspace_id})")

            for item in ITEMS_TO_DEPLOY:
                try:
                    if "notebook" in item:
                        deploy_notebook(token, workspace_id, item)
                    elif "lakehouses" in item:
                        deploy_table(token, workspace_id)
                except Exception as e:
                    logging.error(f"❌ Deployment failed for {item}: {str(e)}")
                    continue

        logging.info("\n🎉 Deployment completed! Check fabric_deployment.log for details")

    except Exception as e:
        logging.error(f"\n💥 FATAL ERROR: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
