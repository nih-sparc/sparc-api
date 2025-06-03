from app.config import Config
import asyncio
import httpx
import json
import threading

# These are the only workspaces/consortia that we are taking protocol metrics from right now. We may want to move this to an env var as the list expands in the future
workspace_ids = ["sparc","re-join","precision-human-pain"]

async def fetch_views_for_workspace(workspace_id):
    views = 0
    page = 1
    page_size = 20

    async with httpx.AsyncClient(timeout=60.0) as client:
        while True:
            url = f"{Config.PROTOCOLS_IO_HOST}/api/v3/workspaces/{workspace_id}/protocols?page_id={page}&page_size={page_size}"
            try:
                res = await client.get(url, headers={"Authorization": f"Bearer {Config.PROTOCOLS_IO_TOKEN}"})
                res.raise_for_status()
                data = res.json()
                items = data.get("items", [])
                if not items:
                    break
                views += sum(p.get("stats", {}).get("number_of_views", 0) for p in items)
                if len(items) < page_size:
                    break
                page += 1
            except Exception as e:
                print(f"[ERROR] Workspace {workspace_id}, Page {page}: {e}")
                break
    return views

async def compute_total_views():
    tasks = [fetch_views_for_workspace(wid) for wid in workspace_ids]
    results = await asyncio.gather(*tasks)
    total = sum(results)
    return total

def execute_protocol_metrics_update(table):
    print("Starting background job to update protocol metrics...")
    total_views = asyncio.run(compute_total_views())
    table_state = get_protocol_metrics_table_state(table)
    table_state["total_protocol_views"] = total_views
    table.updateState(Config.PROTOCOL_METRICS_TABLENAME, json.dumps(table_state), True)
    print("Finished updating protocol metrics.")

def update_protocol_metrics(table):
    thread = threading.Thread(target=execute_protocol_metrics_update, args=(table, ), daemon=True)
    thread.start()
    return

def get_protocol_metrics_table_state(table):
    default_data = {
      'total_protocol_views': -1
    }
    if table is None:
        return default_data
    try:
        current_state = table.pullState(Config.PROTOCOL_METRICS_TABLENAME)
        if current_state is None:
            current_state = table.updateState(Config.PROTOCOL_METRICS_TABLENAME, json.dumps(default_data), True)
        return json.loads(current_state)
    except:
        return default_data
