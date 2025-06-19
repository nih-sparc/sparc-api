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
                print(f"{workspace_id} cummulative views = {views}")
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
    print(f"Workspace total views results = {results}")
    total = sum(results)
    return total

def execute_protocol_metrics_update():
    from app.dbtable import ProtocolMetricsTable  # import here to avoid circular issues

    print("Starting job to update protocol metrics...")
    total_views = asyncio.run(compute_total_views())
    print(f"Total protocol views = {total_views}")

    # RECREATE fresh DB connection
    db_url = Config.DATABASE_URL
    if db_url and db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    table = ProtocolMetricsTable(db_url)
    total_views_data = {
        'total_protocol_views': total_views
    }

    print(f"Updating DB protocol metrics to: {total_views_data}")
    table.updateState(Config.PROTOCOL_METRICS_TABLENAME, json.dumps(total_views_data), True)
    print(f"Finished updating DB. Protocol metrics set to: {table.pullState(Config.PROTOCOL_METRICS_TABLENAME)}")

def update_protocol_metrics():
    thread = threading.Thread(target=execute_protocol_metrics_update)
    thread.start()

def get_protocol_metrics_table_state(table):
    print(f"Retreiving protocol metrics from DB")
    if table is None:
        print(f"Protocol metrics table was None")
        return None
    try:
        current_state = table.pullState(Config.PROTOCOL_METRICS_TABLENAME)
        if current_state is None:
            print("Protocol metrics pullState returned None")
            return None
        print(f"Retreived the following protocol metrics from DB: {current_state}")
        return json.loads(current_state)
    except Exception as e:
        print(f"Error retreiving protocol metrics: {e}")
        return None
