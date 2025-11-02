import configparser
import sys
from datetime import datetime
import pytz
import requests
from requests.auth import HTTPBasicAuth
import urllib3
import json
import time
import re
import os

# --- Configuration ---
config = configparser.ConfigParser()
config.read("segredo.ini")

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Get Pipefy and Chase details
pipefy_api_url = config.get("Pipefy", "pipefy_api_url")
pipefy_api_token = config.get("Pipefy", "pipefy_api_token")
CHASE_URL = config.get("Chase", "CHASE_URL_LIVE")
CHASE_USERNAME = config.get("Chase", "CHASE_USERNAME_LIVE")
CHASE_PASSWORD = config.get("Chase", "CHASE_PASSWORD_LIVE")

# --- Pipefy Database Table IDs ---
BU_AGENCY_TABLE_ID = "306759746"
DIVISIONS_TABLE_ID = "306760306"

# Map ConfigID (as string) to the corresponding Product Table ID
AGENCY_PRODUCT_TABLE_MAP = {
    "1": "306766430",  # Mc Saatchi
    "6": "306759773",  # Up and Up
    "7": "306759851",  # Dalmatian
    "8": "306766722",  # Razor
    "9": "306759853",  # Levergy
}

# Cache for Done phase IDs by table
DONE_PHASE_CACHE = {}

# API Endpoints
API_CONFIGS = config.get("Chase_API_Endpoints", "CONFIGS", fallback="/api/Config")
API_CLIENTS = config.get("Chase_API_Endpoints", "CLIENTS", fallback="/api/Client")
API_PRODUCTS = config.get("Chase_API_Endpoints", "PRODUCTS", fallback="/api/Product")
API_BUSINESS_UNITS = config.get("Chase_API_Endpoints", "BUSINESS_UNITS", fallback="/api/BusinessUnit")
API_DIVISIONS = config.get("Chase_API_Endpoints", "DIVISIONS", fallback="/API/Client/Divisions") 

# Other globals
tz = pytz.timezone("Africa/Johannesburg")
session = requests.Session()
chase_auth = HTTPBasicAuth(CHASE_USERNAME, CHASE_PASSWORD)

def log(message):
    """Simple logger with timestamp."""
    timestamp = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def chase_api_get(endpoint, config_id=None, params=None):
    """Generic function to handle Chase GET requests with retries."""
    url = f"{CHASE_URL}{endpoint}"
    headers = {}
    if config_id:
        headers["ConfigID"] = str(config_id)

    max_retries = 3
    delay = 5
    for attempt in range(max_retries):
        try:
            response = session.get(url, auth=chase_auth, headers=headers, params=params, timeout=30, verify=False)
            response.raise_for_status()
            if response.content:
                return response.json()
            else:
                log(f"Warning: Chase API returned empty response for {endpoint} (ConfigID: {config_id}, Attempt: {attempt+1})")
                return []
        except requests.exceptions.Timeout:
            log(f"Warning: Chase API call timed out for {endpoint} (ConfigID: {config_id}, Attempt: {attempt+1}). Retrying in {delay}s...")
        except requests.exceptions.RequestException as e:
            log(f"ERROR: Chase API call failed for {endpoint} (ConfigID: {config_id}, Attempt: {attempt+1}). Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}")
            if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                return None
        except json.JSONDecodeError:
            log(f"ERROR: Chase API returned non-JSON response for {endpoint} (ConfigID: {config_id}, Attempt: {attempt+1}). Response: {response.text[:200]}")
            return None

        if attempt < max_retries - 1:
            time.sleep(delay)
            delay *= 2
        else:
            log(f"ERROR: Chase API call failed after {max_retries} attempts for {endpoint} (ConfigID: {config_id}).")
            return None

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

def get_done_phase_id(table_id):
    """Fetches the 'Done' phase ID for a given table. Returns None if not found."""
    if table_id in DONE_PHASE_CACHE:
        return DONE_PHASE_CACHE[table_id]
    
    log(f"Fetching 'Done' phase ID for table {table_id}...")
    
    query = """
    query ($table_id: ID!) {
      table(id: $table_id) {
        phases {
          id
          name
        }
      }
    }
    """
    
    payload = {
        "query": query,
        "variables": {"table_id": table_id}
    }
    
    response = pipefy_post(payload)
    
    if not response or "errors" in response:
        log(f"ERROR: Failed to fetch phases for table {table_id}. Response: {response}")
        return None
    
    phases = response.get("data", {}).get("table", {}).get("phases", [])
    
    # Look for a phase named "Done" (case-insensitive)
    done_phase = None
    for phase in phases:
        phase_name = phase.get("name", "").strip().lower()
        if phase_name == "done":
            done_phase = phase.get("id")
            break
    
    if done_phase:
        log(f"Found 'Done' phase ID for table {table_id}: {done_phase}")
        DONE_PHASE_CACHE[table_id] = done_phase
    else:
        log(f"WARNING: No 'Done' phase found for table {table_id}. Available phases: {[p.get('name') for p in phases]}")
        DONE_PHASE_CACHE[table_id] = None
    
    return done_phase

def get_pipefy_table_records(table_id, fields_to_get=None):
    """Fetches all records from the specified Pipefy table with pagination."""
    log(f"Fetching records from Pipefy table: {table_id}")
    all_records = []
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
            record_fields {
              field { id }
              value
            }
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
            log(f"ERROR: Failed to fetch page {page_count} from Pipefy table {table_id}. Aborting fetch. Response: {response}")
            return None

        data = response.get("data", {}).get("table_records", {})
        edges = data.get("edges", [])

        for edge in edges:
            node = edge.get("node", {})
            record_id = node.get("id")
            fields_list = node.get("record_fields", [])

            record_data = {"pipefy_record_id": record_id, "title": node.get("title")}
            for field in fields_list:
                slug = field.get("field", {}).get("id")
                value = field.get("value")
                if slug:
                    record_data[slug] = value
            all_records.append(record_data)

        page_info = data.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")
        log(f"Page {page_count} fetched for {table_id}. hasNextPage: {has_next_page}. Records so far: {len(all_records)}")
        if has_next_page:
            time.sleep(1)

    log(f"Total Pipefy records fetched for {table_id}: {len(all_records)}")
    return all_records

def sanitize_graphql_string(value):
    """Escapes characters that break GraphQL strings and normalizes whitespace."""
    if value is None:
        return ""
    normalized = ' '.join(str(value).split())
    return normalized.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')

def build_pipefy_mutations(records, table_id, action, field_mapping):
    """Builds GraphQL mutations using field_id and value, and correct return structure."""
    mutations = []
    key_identifiers = field_mapping.get("keys", [])
    
    # Define keys that are used for the 'title' and should be excluded from field updates
    title_keys = ["product_name", "business_unit_name", "division_name"]

    for i, record in enumerate(records):
        alias_action = action.replace("-", "_")
        # Generate a unique suffix for the mutation alias
        key_values = [str(record.get(k, '')) for k in key_identifiers]
        unique_suffix = f"{alias_action}_{i}_{'_'.join(key_values)}"
        unique_suffix = re.sub(r'\W|^(?=\d)', '_', unique_suffix)
        
        pipefy_record_id = record.get("pipefy_record_id")

        if action == "create":
            if not all(str(record.get(k)) for k in key_identifiers if k in record):
                log(f"Skipping record creation for table {table_id}: Missing key identifier(s) in {record}")
                continue
            
            fields_to_set = {
                field_mapping["fields"].get(key): sanitize_graphql_string(record.get(key))
                for key in field_mapping["fields"] if key in record
            }
            fields_to_set["status"] = "Active"
            
            # Determine the title value
            title_value = ""
            if "product_name" in record:
                title_value = sanitize_graphql_string(record.get("product_name"))
            elif "business_unit_name" in record:
                title_value = sanitize_graphql_string(record.get("business_unit_name"))
            elif "division_name" in record:
                title_value = sanitize_graphql_string(record.get("division_name"))
            
            # Build fields_attributes array
            fields_attributes_parts = []
            for pipefy_slug, val in fields_to_set.items():
                fields_attributes_parts.append(f'{{field_id: "{pipefy_slug}", field_value: "{val}"}}')
            fields_attributes_string = f"[{', '.join(fields_attributes_parts)}]"
            
            mutation_body = f'createTableRecord(input: {{table_id: "{table_id}", title: "{title_value}", fields_attributes: {fields_attributes_string} }}) {{ table_record {{ id }} }}'
            mutations.append(f"mut_{unique_suffix}: {mutation_body}")

        elif action == "update":
            if not pipefy_record_id:
                log(f"Skipping record update: Missing 'pipefy_record_id'")
                continue
            
            # Exclude title keys from the fields_to_set dictionary
            fields_to_set = {
                field_mapping["fields"].get(key): sanitize_graphql_string(record.get(key))
                for key in field_mapping["fields"]
                if key not in key_identifiers and key not in title_keys and key in record
            }
            fields_to_set["status"] = sanitize_graphql_string(record.get("status", "Active"))

            # Check if title needs updating
            current_title = record.get("current_title", "")
            new_title = ""
            if "product_name" in record:
                new_title = sanitize_graphql_string(record.get("product_name"))
            elif "business_unit_name" in record:
                new_title = sanitize_graphql_string(record.get("business_unit_name"))
            elif "division_name" in record:
                new_title = sanitize_graphql_string(record.get("division_name"))
            
            title_needs_update = new_title and current_title != new_title
            
            # Build combined update mutation that handles both fields and title in one call
            field_values = []
            for pipefy_slug, val in fields_to_set.items():
                if val is not None: 
                    field_values.append(f'{{fieldId: "{pipefy_slug}", value: "{val}"}}')
            
            # If we have fields to update OR title to update, create a single mutation
            if field_values or title_needs_update:
                if field_values:
                    field_values_string = f"[{', '.join(field_values)}]"
                    mutation_body = f'updateFieldsValues(input: {{nodeId: "{pipefy_record_id}", values: {field_values_string} }}) {{ clientMutationId }}'
                    mutations.append(f"mut_{unique_suffix}: {mutation_body}")
                
                # Add separate title update only if needed
                if title_needs_update:
                    title_mutation = f'updateTableRecord(input: {{id: "{pipefy_record_id}", title: "{new_title}"}}) {{ table_record {{ id }} }}'
                    mutations.append(f"mut_{unique_suffix}_ttl: {title_mutation}")
            else:
                log(f"Skipping update for record {pipefy_record_id}, no changes detected.")

        elif action == "archive":
            if not pipefy_record_id:
                log(f"Skipping record archive: Missing 'pipefy_record_id'")
                continue
            
            # Get the Done phase ID for this table
            done_phase_id = get_done_phase_id(table_id)
            
            if done_phase_id:
                # Move record to Done phase using updateTableRecordField
                mutation_body = f'updateTableRecordField(input: {{table_record_id: "{pipefy_record_id}", field_id: "current_phase", new_value: "{done_phase_id}"}}) {{ table_record {{ id }} }}'
                mutations.append(f"mut_{unique_suffix}: {mutation_body}")
            else:
                # Fallback: Just update status to Archived if no Done phase found
                log(f"WARNING: No Done phase found for table {table_id}. Using status field fallback for record {pipefy_record_id}")
                mutation_body = f'updateFieldsValues(input: {{nodeId: "{pipefy_record_id}", values: [{{fieldId: "status", value: "Archived"}}] }}) {{ clientMutationId }}'
                mutations.append(f"mut_{unique_suffix}: {mutation_body}")

    return mutations

def execute_pipefy_mutations(mutations):
    """Executes a batch of mutations in Pipefy, handling batching."""
    if not mutations:
        log("No mutations to execute.")
        return True
    
    batch_size = 50
    all_successful = True
    
    for i in range(0, len(mutations), batch_size):
        batch = mutations[i:i+batch_size]
        full_mutation_string = f"mutation {{ {' '.join(batch)} }}"
        payload = {"query": full_mutation_string}
        log(f"Executing batch {i//batch_size + 1}/{(len(mutations) + batch_size - 1)//batch_size} ({len(batch)} mutations)...")
        
        # Debug: Log the actual mutation for small batches
        if len(batch) <= 3:
            log(f"DEBUG: Mutation query: {full_mutation_string[:500]}...")
        
        response = pipefy_post(payload)
        
        if not response or "errors" in response:
            log(f"ERROR: Pipefy mutation batch {i//batch_size + 1} failed. Response: {response}")
            all_successful = False
            if response and "errors" in response:
                for error in response["errors"]:
                    log(f"  > GraphQL Error: {error.get('message')}")
                    # Log the path if available to identify which mutation failed
                    if "path" in error:
                        log(f"    Error path: {error.get('path')}")
            continue
        else:
            data = response.get("data", {})
            success_count = 0
            null_count = 0
            fail_aliases = []
            
            for alias, result in data.items():
                if result is None:
                    null_count += 1
                    fail_aliases.append(alias)
                elif isinstance(result, dict) and (result.get("table_record") or result.get("clientMutationId") is not None):
                    success_count += 1
                else:
                    # Unexpected result structure
                    null_count += 1
                    fail_aliases.append(alias)
            
            if success_count == len(batch):
                log(f"Batch {i//batch_size + 1} completed successfully. All {success_count} mutations succeeded.")
            else:
                log(f"Batch {i//batch_size + 1} finished with partial success. Success: {success_count}/{len(batch)}, Failed: {null_count}")
                all_successful = False
                if fail_aliases:
                    log(f"  Failed mutation aliases (first 10): {fail_aliases[:10]}")
        
        if i + batch_size < len(mutations):
            time.sleep(2)
    
    if all_successful:
        log("All Pipefy mutation batches executed successfully.")
    else:
        log("WARNING: One or more Pipefy mutation batches had failures.")
    return all_successful

def sync_table(chase_data_list, pipefy_table_id, field_mapping):
    """Syncs a specific list of Chase data to a specific Pipefy table."""
    log(f"--- Starting sync for Table ID: {pipefy_table_id} ---")

    pipefy_fields_to_get = list(field_mapping["fields"].values())
    pipefy_data = get_pipefy_table_records(pipefy_table_id, pipefy_fields_to_get)
    if pipefy_data is None:
        log(f"ERROR: Cannot proceed with sync for table {pipefy_table_id} due to fetch error.")
        return False

    key_fields = field_mapping["keys"]
    
    def get_key(record, is_pipefy=False):
        key_parts = []
        for k in key_fields:
            pipefy_slug = field_mapping["fields"].get(k)
            value = record.get(pipefy_slug if is_pipefy else k, '')
            key_parts.append(str(value))
        
        # Special key for divisions: division_id-customer_id
        if pipefy_table_id == DIVISIONS_TABLE_ID:
             key_parts = [
                 str(record.get(field_mapping['fields']['division_id'] if is_pipefy else 'division_id', '')),
                 str(record.get(field_mapping['fields']['customer_id'] if is_pipefy else 'customer_id', ''))
             ]
        
        return "-".join(key_parts)

    chase_map = {get_key(r): r for r in chase_data_list}
    pipefy_map = {get_key(r, is_pipefy=True): r for r in pipefy_data}

    to_create = []
    to_update = []
    to_archive = []
    
    title_keys = ["product_name", "business_unit_name", "division_name"]

    for chase_key, chase_record in chase_map.items():
        if chase_key not in pipefy_map:
            to_create.append(chase_record)
        else:
            pipefy_record = pipefy_map[chase_key]
            needs_update = False
            
            for record_key, pipefy_slug in field_mapping["fields"].items():
                if record_key not in key_fields:
                    chase_val = sanitize_graphql_string(chase_record.get(record_key))
                    pipefy_val = pipefy_record.get(pipefy_slug)
                    
                    if pipefy_val is None:
                        pipefy_val = ""
                    else:
                        pipefy_val = ' '.join(str(pipefy_val).split())
                    
                    if chase_val != pipefy_val:
                        needs_update = True
                        break
            
            # Check title
            current_title = pipefy_record.get('title', '')
            new_title = ""
            if "product_name" in chase_record:
                new_title = sanitize_graphql_string(chase_record.get("product_name"))
            elif "business_unit_name" in chase_record:
                new_title = sanitize_graphql_string(chase_record.get("business_unit_name"))
            elif "division_name" in chase_record:
                new_title = sanitize_graphql_string(chase_record.get("division_name"))
            
            if new_title and current_title != new_title:
                needs_update = True
            
            if pipefy_record.get('status') != "Active":
                needs_update = True

            if needs_update:
                update_data = chase_record.copy()
                update_data['pipefy_record_id'] = pipefy_record['pipefy_record_id']
                update_data['current_title'] = current_title
                update_data['status'] = "Active"
                to_update.append(update_data)

    for pipefy_key, pipefy_record in pipefy_map.items():
        if pipefy_key not in chase_map and pipefy_record.get('status') != "Archived":
            to_archive.append(pipefy_record)

    log(f"Sync Results for {pipefy_table_id}: Create: {len(to_create)}, Update: {len(to_update)}, Archive: {len(to_archive)}")

    create_mutations = build_pipefy_mutations(to_create, pipefy_table_id, "create", field_mapping)
    update_mutations = build_pipefy_mutations(to_update, pipefy_table_id, "update", field_mapping)
    archive_mutations = build_pipefy_mutations(to_archive, pipefy_table_id, "archive", field_mapping)

    success_c = execute_pipefy_mutations(create_mutations)
    success_u = execute_pipefy_mutations(update_mutations)
    success_a = execute_pipefy_mutations(archive_mutations)

    log(f"--- Finished sync for Table ID: {pipefy_table_id} ---")
    return success_c and success_u and success_a

def main():
    log("===== STARTING CHASE-PIPEFY MULTI-TABLE SYNC =====")

    bu_agency_field_map = {
        "keys": ["config_id", "business_unit_id"],
        "fields": {
            "config_id": "config_id",
            "business_unit_id": "business_unit_id",
            "business_unit_name": "business_unit_name",
        }
    }
    
    product_client_field_map = {
        "keys": ["config_id", "client_id", "product_id"],
        "fields": {
            "config_id": "config_id",
            "client_id": "customer_id",
            "client_name": "customer_name",
            "product_id": "product_id",
            "product_name": "product",
            "contact_name": "client_contact_name",
        }
    }

    divisions_field_map = {
        "keys": ["division_id", "customer_id"],
        "fields": {
            "division_id": "division_id",
            "division_name": "division_name",
            "customer_id": "customer_id",
            "customer_name": "customer_name", 
        }
    }

    configs = chase_api_get(API_CONFIGS)
    if configs is None or not isinstance(configs, list):
        log("ERROR: Could not fetch Configs. Aborting.")
        sys.exit(1)
    log(f"Found {len(configs)} Configs.")

    log("Building master client map...")
    client_id_to_name_map = {}
    for config in configs:
        config_id = config.get("ConfigID")
        if not config_id:
            continue
        clients = chase_api_get(API_CLIENTS, config_id=config_id)
        if isinstance(clients, list):
            for client in clients:
                if client.get("ClientID") and client.get("ClientName"):
                    client_id_to_name_map[client.get("ClientID")] = client.get("ClientName")
    log(f"Master client map built with {len(client_id_to_name_map)} unique clients.")

    all_business_units = []
    all_products_clients_by_config = {cfg_id: [] for cfg_id in AGENCY_PRODUCT_TABLE_MAP.keys()}
    all_divisions = []
    overall_success = True

    for config in configs:
        config_id = config.get("ConfigID")
        config_name = config.get("CompanyName")
        if not config_id:
            continue

        config_id_str = str(config_id)

        # --- Business Units logic ---
        business_units = chase_api_get(API_BUSINESS_UNITS, config_id=config_id)
        if isinstance(business_units, list):
            for bu in business_units:
                bu_data = {
                    "config_id": config_id,
                    "business_unit_id": bu.get("BusinessUnitID"),
                    "business_unit_name": bu.get("BusinessUnit"),
                    "status": "Active"
                }
                if bu_data["business_unit_id"]:
                    all_business_units.append(bu_data)
                else:
                    log(f"Warning: Skipping BU with missing ID for Config {config_id}")
        else:
            log(f"Warning: Failed to fetch or invalid format for BUs, ConfigID {config_id}")
            overall_success = False

        # --- Divisions logic ---
        divisions = chase_api_get(API_DIVISIONS, config_id=config_id)
        if isinstance(divisions, list):
            for div in divisions:
                customer_id = div.get("CustomerID")
                division_id = div.get("DivisionID")
                division_name = div.get("DivisionName")
                
                if not (customer_id and division_id and division_name):
                    log(f"Warning: Skipping Division with missing ID/CustomerID/Name for Config {config_id}")
                    continue

                div_data = {
                    "division_id": division_id,
                    "division_name": division_name,
                    "customer_id": customer_id,
                    "customer_name": client_id_to_name_map.get(customer_id),
                    "status": "Active"
                }
                all_divisions.append(div_data)
        else:
            log(f"Warning: Failed to fetch or invalid format for Divisions, ConfigID {config_id}")
            overall_success = False

        # --- Product/Client logic ---
        if config_id_str not in AGENCY_PRODUCT_TABLE_MAP:
            log(f"Info: No product table mapping found for ConfigID {config_id_str}. Skipping product/client sync for this config.")
            continue

        clients = chase_api_get(API_CLIENTS, config_id=config_id)
        if isinstance(clients, list):
            log(f"Processing {len(clients)} clients for ConfigID {config_id}...")
            for client in clients:
                client_id = client.get("ClientID")
                if not client_id:
                    continue

                products = chase_api_get(f"{API_PRODUCTS}/CustomerID/{client_id}", config_id=config_id)
                if isinstance(products, list):
                    if not products:
                        log(f"Info: No products for Client {client_id}, Config {config_id}")
                    for product in products:
                        prod_id = product.get("ProductID")
                        if not prod_id:
                            continue
                        prod_client_data = {
                            "config_id": config_id,
                            "client_id": client_id,
                            "client_name": client.get("ClientName"),
                            "product_id": prod_id,
                            "product_name": product.get("ProductName"),
                            "contact_name": product.get("ContactName"),
                            "status": "Active"
                        }
                        all_products_clients_by_config[config_id_str].append(prod_client_data)
                else:
                    log(f"Warning: Failed to fetch or invalid format for Products, Client {client_id}, Config {config_id}")
                    overall_success = False
        else:
            log(f"Warning: Failed to fetch or invalid format for Clients, ConfigID {config_id}")
            overall_success = False

    # --- Sync Business Units ---
    log(f"Collected {len(all_business_units)} total BU records.")
    success_bu = sync_table(all_business_units, BU_AGENCY_TABLE_ID, bu_agency_field_map)
    if not success_bu:
        overall_success = False

    # --- Sync Divisions ---
    log(f"Collected {len(all_divisions)} total Division records.")
    success_div = sync_table(all_divisions, DIVISIONS_TABLE_ID, divisions_field_map)
    if not success_div:
        overall_success = False

    # --- Sync Products/Clients ---
    for config_id_str, product_client_list in all_products_clients_by_config.items():
        target_table_id = AGENCY_PRODUCT_TABLE_MAP.get(config_id_str)
        if target_table_id:
            log(f"Collected {len(product_client_list)} Product/Client records for ConfigID {config_id_str}.")
            success_prod = sync_table(product_client_list, target_table_id, product_client_field_map)
            if not success_prod:
                overall_success = False
        else:
            log(f"Error: Product table ID missing for ConfigID {config_id_str} during sync execution.")
            overall_success = False

    if overall_success:
        log("===== SYNC COMPLETE SUCCESSFULLY =====")
    else:
        log("===== SYNC COMPLETE WITH WARNINGS =====")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        error_details = f"CRITICAL ERROR: Unhandled exception in main execution block: {e} in {fname} line {exc_tb.tb_lineno}"
        log(error_details)
        sys.exit(1)