import configparser
import sys
from datetime import datetime
import pytz
import requests
import urllib3
import json
import time

# --- Configuration ---
config = configparser.ConfigParser()
config.read("segredo.ini")

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get Pipefy details
pipefy_api_url = config.get("Pipefy", "pipefy_api_url")
pipefy_api_token = config.get("Pipefy", "pipefy_api_token")

# Other globals
tz = pytz.timezone("Africa/Johannesburg")
session = requests.Session()

def log(message):
    """Simple logger with timestamp."""
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def pipefy_post(payload):
    """Generic function to handle Pipefy GraphQL POST requests with retries."""
    max_retries = 3
    delay = 5
    for attempt in range(max_retries):
        try:
            response = session.post(
                pipefy_api_url,
                headers={
                    "Authorization": f"Bearer {pipefy_api_token}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            if "errors" in result:
                log(f"ERROR: Pipefy GraphQL Error (Attempt {attempt+1}). Errors: {result['errors']}")
                if attempt == max_retries - 1:
                    return result
            else:
                return result
        except requests.exceptions.Timeout:
            log(f"Warning: Pipefy API call timed out (Attempt {attempt+1}). Retrying in {delay}s...")
        except requests.exceptions.RequestException as e:
            log(f"ERROR: Pipefy API request failed (Attempt {attempt+1}). Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}")
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                return {"errors": [{"message": f"Pipefy Client Error: {e}"}]}
        except json.JSONDecodeError:
            log(f"ERROR: Pipefy API returned non-JSON response (Attempt {attempt+1}). Response: {response.text[:200]}")
            return {"errors": [{"message": "Pipefy returned non-JSON response"}]}
        
        if attempt < max_retries - 1:
            time.sleep(delay)
            delay *= 2
        else:
            log(f"ERROR: Pipefy API call failed after {max_retries} attempts.")
            return {"errors": [{"message": "Pipefy API call failed after multiple retries"}]}

def get_pipefy_table_records(table_id):
    """Fetches all record IDs from the specified Pipefy table with pagination."""
    log(f"Fetching records from Pipefy table: {table_id}")
    all_record_ids = []
    has_next_page = True
    cursor = None

    query_template = """
    query ($table_id: ID!, $cursor: String) {
      table_records(table_id: $table_id, after: $cursor, first: 50) {
        pageInfo { endCursor hasNextPage }
        edges {
          node {
            id
            title
          }
        }
      }
    }
    """

    page_count = 0
    while has_next_page:
        page_count += 1
        log(f"Fetching page {page_count} for table {table_id}...")
        variables = {"table_id": table_id}
        if cursor:
            variables["cursor"] = cursor

        payload = {"query": query_template, "variables": variables}
        response = pipefy_post(payload)

        if not response or "errors" in response:
            log(f"ERROR: Failed to fetch page {page_count} from Pipefy table {table_id}. Response: {response}")
            return None

        data = response.get("data", {}).get("table_records", {})
        edges = data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            record_id = node.get("id")
            title = node.get("title", "")
            if record_id:
                all_record_ids.append({"id": record_id, "title": title})

        page_info = data.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
        log(f"Page {page_count} fetched. hasNextPage: {has_next_page}. Records so far: {len(all_record_ids)}")
        if has_next_page:
            time.sleep(1)

    log(f"Total records fetched for {table_id}: {len(all_record_ids)}")
    return all_record_ids

def delete_all_table_records(table_id):
    """Deletes all records from a Pipefy table."""
    log(f"===== STARTING DELETE FOR TABLE {table_id} =====")
    
    # First, fetch all record IDs
    all_records = get_pipefy_table_records(table_id)
    if not all_records:
        log(f"No records to delete or error fetching records from table {table_id}")
        return False
    
    record_ids = [rec["id"] for rec in all_records]
    log(f"Found {len(record_ids)} records to delete from table {table_id}")
    
    # Ask for confirmation
    print(f"\n{'='*60}")
    print(f"WARNING: You are about to delete {len(record_ids)} records!")
    print(f"Table ID: {table_id}")
    if all_records:
        print(f"Sample titles: {', '.join([rec['title'][:30] for rec in all_records[:3]])}")
    print(f"{'='*60}\n")
    
    confirmation = input("Type 'DELETE' to confirm deletion: ")
    if confirmation != "DELETE":
        log("Deletion cancelled by user.")
        return False
    
    # Build delete mutations
    delete_mutations = []
    for i, record_id in enumerate(record_ids):
        alias = f"del_{i}"
        mutation = f'{alias}: deleteTableRecord(input: {{id: "{record_id}"}}) {{ success }}'
        delete_mutations.append(mutation)
    
    # Execute deletions in batches
    batch_size = 50
    all_successful = True
    total_deleted = 0
    
    for i in range(0, len(delete_mutations), batch_size):
        batch = delete_mutations[i:i+batch_size]
        full_mutation = f"mutation {{ {' '.join(batch)} }}"
        payload = {"query": full_mutation}
        
        log(f"Deleting batch {i//batch_size + 1}/{(len(delete_mutations) + batch_size - 1)//batch_size} ({len(batch)} records)...")
        
        response = pipefy_post(payload)
        
        if not response or "errors" in response:
            log(f"ERROR: Failed to delete batch {i//batch_size + 1}. Response: {response}")
            all_successful = False
        else:
            data = response.get("data", {})
            success_count = sum(1 for result in data.values() if result and result.get("success"))
            total_deleted += success_count
            log(f"Batch {i//batch_size + 1} deleted. Success: {success_count}/{len(batch)}")
        
        if i + batch_size < len(delete_mutations):
            time.sleep(2)
    
    if all_successful:
        log(f"===== SUCCESSFULLY DELETED {total_deleted} RECORDS FROM TABLE {table_id} =====")
    else:
        log(f"===== DELETION COMPLETED WITH WARNINGS. Deleted {total_deleted}/{len(record_ids)} records =====")
    
    return all_successful

def main():
    """Main function for command-line usage."""
    if len(sys.argv) > 1:
        table_id = sys.argv[1]
        delete_all_table_records(table_id)
    else:
        print("Usage: python delete_pipefy_table.py <table_id>")
        print("Example: python delete_pipefy_table.py 306766430")
        sys.exit(1)

if __name__ == "__main__":
    main()