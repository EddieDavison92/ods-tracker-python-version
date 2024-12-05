import requests
import json
from datetime import datetime
import os
import pandas as pd
from tqdm import tqdm
from collections import defaultdict

class ODSClient:
    def __init__(self):
        self.base_url = "https://directory.spineservices.nhs.uk/ORD/2-0-0"
        self.headers = {"Accept": "application/json"}

    def get_related_orgs(self, ods_code):
        """Get all organisations related to the given ODS code using API pagination"""
        all_orgs = []
        current_url = f"{self.base_url}/organisations"
        first_request = True
        params = {
            "RelTypeId": "RE2,RE3,RE4,RE5,RE6,RE7,RE8,RE9,RE10,RE11",
            "TargetOrgId": ods_code,
            "_format": "json"
        }
        
        # Make first request to get total count
        try:
            response = requests.get(current_url, params=params, headers=self.headers)
            response.raise_for_status()
            total_count = int(response.headers.get('X-Total-Count', 0))
            
            # Create progress bar
            pbar = tqdm(total=total_count, desc="Retrieving organisations")
            
            while True:
                if first_request:
                    orgs = response.json().get("Organisations", [])
                    first_request = False
                else:
                    response = requests.get(current_url, headers=self.headers)
                    response.raise_for_status()
                    orgs = response.json().get("Organisations", [])
                
                if not orgs:
                    break
                    
                all_orgs.extend(orgs)
                pbar.update(len(orgs))
                
                next_page = response.headers.get('next-page')
                if not next_page:
                    break
                    
                current_url = next_page
                
        except requests.exceptions.RequestException as e:
            print(f"Error getting related organisations: {e}")
            if 'response' in locals():
                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {response.headers}")
                print(f"Response content: {response.text[:500]}...")
        finally:
            if 'pbar' in locals():
                pbar.close()
        
        return all_orgs

    def get_org_details(self, ods_code):
        """Get full details for a specific organization using the organization endpoint"""
        url = f"{self.base_url}/organisations/{ods_code}"
        params = {"_format": "json"}
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error getting organisation details for {ods_code}: {e}")
            return None

    def get_practices_and_pcns(self, icb_code):
        """Get all GP Practices and PCNs for specific ICB"""
        url = f"{self.base_url}/organisations"
        params = {
            "RelTypeId": "RE4,RE6",  # IS COMMISSIONED BY, IS OPERATED BY
            "TargetOrgId": icb_code,  # Filter for specific ICB
            "_format": "json"
        }
        
        all_orgs = []
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            total_count = int(response.headers.get('X-Total-Count', 0))
            
            with tqdm(total=total_count, desc="Retrieving organisations") as pbar:
                while True:
                    orgs = response.json().get("Organisations", [])
                    if not orgs:
                        break
                    
                    all_orgs.extend(orgs)
                    pbar.update(len(orgs))
                    
                    next_page = response.headers.get('next-page')
                    if not next_page:
                        break
                    
                    response = requests.get(next_page, headers=self.headers)
                    response.raise_for_status()
            
            # Get full details and filter for practices and PCNs
            details = {}
            for org in tqdm(all_orgs, desc="Getting organisation details"):
                ods_code = org.get("OrgId")
                if ods_code:
                    org_details = self.get_org_details(ods_code)
                    if org_details:
                        org_info = org_details.get("Organisation", {})
                        roles = org_info.get("Roles", {}).get("Role", [])
                        if not isinstance(roles, list):
                            roles = [roles]
                        
                        # Only include if it's a practice or PCN
                        if any(role.get("id") in ["RO76", "RO272"] for role in roles):
                            details[ods_code] = org_details
            
            return details
                
        except requests.exceptions.RequestException as e:
            print(f"Error getting organisations: {e}")
            if 'response' in locals():
                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {response.headers}")
                print(f"Response content: {response.text[:500]}...")
            return {}

def load_or_fetch_data(icb_code):
    """Load existing data for today or fetch new data if none exists"""
    today = datetime.now().strftime("%Y%m%d")
    json_filename = f"ncl_icb_data_{today}.json"
    json_path = os.path.join("data", json_filename)
    
    if os.path.exists(json_path):
        print(f"Loading existing data from {json_filename}")
        with open(json_path, 'r') as f:
            return json.load(f)
    
    print("No existing data found for today, fetching new data...")
    client = ODSClient()
    
    # Get practices and PCNs
    organisations = client.get_practices_and_pcns(icb_code)
    
    full_data = {
        "metadata": {
            "icb_code": icb_code,
            "download_date": datetime.now().isoformat(),
            "total_organisations": len(organisations)
        },
        "organisations": organisations
    }
    
    # Save the data
    os.makedirs("data", exist_ok=True)
    with open(json_path, 'w') as f:
        json.dump(full_data, f, indent=2)
    
    return full_data

def create_practice_pcn_report(data, filename):
    """Create Excel report focusing on GP Practices and PCNs"""
    if not data.get("organisations"):
        print("No organization data available to create report")
        return
        
    excel_path = os.path.join("data", filename)
    writer = pd.ExcelWriter(excel_path, engine='openpyxl')
    
    practices = []
    pcns = {}  # Use dict for easier lookup
    
    print("Processing organisations...")
    # First identify all PCNs
    for ods_code, org_data in tqdm(data["organisations"].items(), desc="Identifying PCNs"):
        org_info = org_data.get("Organisation", {})
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        if any(role.get("id") == "RO272" and role.get("primaryRole", False) for role in roles):
            location = org_info.get("GeoLoc", {}).get("Location", {})
            dates = org_info.get("Date", [])
            if not isinstance(dates, list):
                dates = [dates]
            
            pcns[ods_code] = {
                'name': org_info.get("Name"),
                'status': org_info.get("Status"),
                'operational_start': next((d.get("Start") for d in dates if d.get("Type") == "Operational"), None),
                'operational_end': next((d.get("End") for d in dates if d.get("Type") == "Operational"), None),
                'legal_start': next((d.get("Start") for d in dates if d.get("Type") == "Legal"), None),
                'legal_end': next((d.get("End") for d in dates if d.get("Type") == "Legal"), None),
                'address': location.get("AddrLn1"),
                'town': location.get("Town"),
                'postcode': location.get("PostCode"),
                'uprn': location.get("UPRN"),
                'member_practices': [],
                'last_changed': org_info.get("LastChangeDate")
            }
    
    # Process GP Practices
    for ods_code, org_data in tqdm(data["organisations"].items(), desc="Processing GP Practices"):
        org_info = org_data.get("Organisation", {})
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        if any(role.get("id") == "RO76" for role in roles):
            location = org_info.get("GeoLoc", {}).get("Location", {})
            dates = org_info.get("Date", [])
            if not isinstance(dates, list):
                dates = [dates]
            
            # Get contact details
            contacts = org_info.get("Contacts", {}).get("Contact", [])
            if not isinstance(contacts, list):
                contacts = [contacts]
            
            phone = next((c.get("value") for c in contacts if c.get("type") == "tel"), None)
            
            # Get all relationships
            rels = org_info.get("Rels", {}).get("Rel", [])
            if not isinstance(rels, list):
                rels = [rels]
            
            # Get PCN relationships (RE8)
            pcn_rels = [rel for rel in rels if rel.get("id") == "RE8"]
            current_pcn = None
            current_pcn_date = None
            pcn_history = []
            
            for rel in pcn_rels:
                target = rel.get("Target", {})
                target_ods = target.get("OrgId", {}).get("extension")
                
                if target_ods in pcns:
                    rel_dates = rel.get("Date", [])
                    if not isinstance(rel_dates, list):
                        rel_dates = [rel_dates]
                    
                    start_date = next((d.get("Start") for d in rel_dates if isinstance(d, dict)), None)
                    end_date = next((d.get("End") for d in rel_dates if isinstance(d, dict)), None)
                    
                    pcn_history.append({
                        'pcn_ods': target_ods,
                        'pcn_name': pcns[target_ods]['name'],
                        'status': rel.get("Status"),
                        'start_date': start_date,
                        'end_date': end_date
                    })
                    
                    if rel.get("Status") == "Active" and not end_date:
                        if not current_pcn or (start_date and start_date > current_pcn_date):
                            current_pcn = target_ods
                            current_pcn_date = start_date
                            
                            # Add to PCN's member practices
                            pcns[target_ods]['member_practices'].append({
                                'ods_code': ods_code,
                                'name': org_info.get("Name"),
                                'start_date': start_date
                            })
            
            # Get role status
            gp_role = next((role for role in roles if role.get("id") == "RO76"), None)
            role_status = gp_role.get("Status") if gp_role else None
            
            # Use the ODS status directly instead of calculating it
            practices.append({
                'ODS Code': ods_code,
                'Name': org_info.get("Name"),
                'Status': org_info.get("Status"),  # Use ODS status directly
                'Primary Role': next((role.get("id") for role in roles if role.get("primaryRole", False)), None),
                'Operational Start': next((d.get("Start") for d in dates if d.get("Type") == "Operational"), None),
                'Operational End': next((d.get("End") for d in dates if d.get("Type") == "Operational"), None),
                'Legal Start': next((d.get("Start") for d in dates if d.get("Type") == "Legal"), None),
                'Legal End': next((d.get("End") for d in dates if d.get("Type") == "Legal"), None),
                'Address': location.get("AddrLn1"),
                'Address Line 2': location.get("AddrLn2"),
                'Town': location.get("Town"),
                'County': location.get("County"),
                'Postcode': location.get("PostCode"),
                'UPRN': location.get("UPRN"),
                'Phone': phone,
                'Current PCN': pcns.get(current_pcn, {}).get('name') if current_pcn else None,
                'Current PCN Code': current_pcn,
                'PCN Member Since': current_pcn_date,
                'PCN History': '; '.join(
                    f"{h['pcn_name']} ({h['pcn_ods']}, {h['status']}, {h['start_date']}-{h['end_date'] if h['end_date'] else 'present'})"
                    for h in sorted(pcn_history, key=lambda x: x['start_date'] or '')
                ) if pcn_history else None,
                'Last Changed': org_info.get("LastChangeDate")
            })
    
    # Create DataFrames
    practices_df = pd.DataFrame(practices)
    practices_df.sort_values(['Status', 'Name'], inplace=True)
    practices_df.to_excel(writer, sheet_name='GP Practices', index=False)
    
    pcns_df = pd.DataFrame([{
        'ODS Code': ods_code,
        'Name': info['name'],
        'Status': info['status'],
        'Operational Start': info['operational_start'],
        'Operational End': info['operational_end'],
        'Legal Start': info['legal_start'],
        'Legal End': info['legal_end'],
        'Address': info['address'],
        'Town': info['town'],
        'Postcode': info['postcode'],
        'UPRN': info['uprn'],
        'Number of Member Practices': len(info['member_practices']),
        'Member Practices': '; '.join(
            f"{p['name']} ({p['ods_code']}, from {p['start_date']})"
            for p in sorted(info['member_practices'], key=lambda x: x['name'])
        ),
        'Last Changed': info['last_changed']
    } for ods_code, info in pcns.items()])
    
    pcns_df.sort_values('Name', inplace=True)
    pcns_df.to_excel(writer, sheet_name='PCNs', index=False)
    
    writer.close()
    print(f"Practice/PCN report saved to {excel_path}")

def analyze_data_structure(data):
    """Analyze and print details about the raw data structure"""
    print("\nAnalyzing Data Structure:")
    print("=========================")
    
    # Count organizations by role
    role_counts = defaultdict(int)
    role_examples = {}
    practice_count = 0
    pcn_count = 0
    active_practices = 0
    inactive_practices = 0
    
    print("\nScanning organizations...")
    for ods_code, org_data in data["organisations"].items():
        org_info = org_data.get("Organisation", {})
        
        # Get roles
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        # Count roles and store examples
        for role in roles:
            role_id = role.get("id")
            is_primary = role.get("primaryRole", False)
            role_counts[f"{role_id} ({'Primary' if is_primary else 'Non-Primary'})"] += 1
            
            if role_id not in role_examples:
                role_examples[role_id] = {
                    'name': org_info.get("Name"),
                    'ods_code': ods_code,
                    'is_primary': is_primary
                }
        
        # Count practices and PCNs
        if any(role.get("id") == "RO76" for role in roles):
            practice_count += 1
            if org_info.get("Status") == "Active":
                active_practices += 1
            else:
                inactive_practices += 1
            
            # If this is a practice, print its full structure
            if practice_count == 1:
                print("\nExample Practice Structure:")
                print(json.dumps(org_info, indent=2))
        
        if any(role.get("id") == "RO272" and role.get("primaryRole", False) for role in roles):
            pcn_count += 1
            # If this is a PCN, print its full structure
            if pcn_count == 1:
                print("\nExample PCN Structure:")
                print(json.dumps(org_info, indent=2))
    
    print("\nSummary:")
    print(f"Total organizations: {len(data['organisations'])}")
    print(f"Total practices: {practice_count} (Active: {active_practices}, Inactive: {inactive_practices})")
    print(f"Total PCNs: {pcn_count}")
    
    print("\nRole distribution:")
    for role, count in sorted(role_counts.items()):
        example = role_examples.get(role.split()[0])
        if example:
            print(f"{role}: {count} organizations")
            print(f"  Example: {example['name']} ({example['ods_code']})")
    
    # Analyze relationships
    print("\nAnalyzing relationships...")
    rel_types = defaultdict(int)
    rel_examples = {}
    
    for ods_code, org_data in data["organisations"].items():
        org_info = org_data.get("Organisation", {})
        rels = org_info.get("Rels", {}).get("Rel", [])
        if not isinstance(rels, list):
            rels = [rels]
        
        for rel in rels:
            rel_id = rel.get("id")
            rel_types[rel_id] += 1
            
            if rel_id not in rel_examples:
                target = rel.get("Target", {})
                rel_examples[rel_id] = {
                    'source': org_info.get("Name"),
                    'source_ods': ods_code,
                    'target_ods': target.get("OrgId", {}).get("extension"),
                    'status': rel.get("Status")
                }
    
    print("\nRelationship types:")
    for rel_id, count in sorted(rel_types.items()):
        example = rel_examples.get(rel_id)
        if example:
            print(f"\n{rel_id}: {count} relationships")
            print(f"  Example: {example['source']} ({example['source_ods']}) -> {example['target_ods']}")
            print(f"  Status: {example['status']}")

def analyze_practice_statuses(data):
    """Analyze practice statuses in detail"""
    print("\nAnalyzing Practice Statuses:")
    print("===========================")
    
    practices = []
    for ods_code, org_data in data["organisations"].items():
        org_info = org_data.get("Organisation", {})
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        if any(role.get("id") == "RO76" for role in roles):
            # Get dates
            dates = org_info.get("Date", [])
            if not isinstance(dates, list):
                dates = [dates]
            
            operational_dates = [d for d in dates if d.get("Type") == "Operational"]
            legal_dates = [d for d in dates if d.get("Type") == "Legal"]
            
            # Get role status
            gp_role = next((role for role in roles if role.get("id") == "RO76"), None)
            role_status = gp_role.get("Status") if gp_role else None
            
            practice = {
                'ods_code': ods_code,
                'name': org_info.get("Name"),
                'org_status': org_info.get("Status"),
                'role_status': role_status,
                'operational_start': next((d.get("Start") for d in operational_dates), None),
                'operational_end': next((d.get("End") for d in operational_dates), None),
                'legal_start': next((d.get("Start") for d in legal_dates), None),
                'legal_end': next((d.get("End") for d in legal_dates), None)
            }
            practices.append(practice)
    
    # Analyze statuses
    print("\nPractice Status Distribution:")
    status_counts = defaultdict(int)
    for p in practices:
        key = f"Org: {p['org_status']}, Role: {p['role_status']}"
        status_counts[key] += 1
    
    for status, count in status_counts.items():
        print(f"{status}: {count} practices")
    
    # Show examples of each status combination
    print("\nExample practices for each status combination:")
    for status_combo in status_counts.keys():
        examples = [p for p in practices 
                   if f"Org: {p['org_status']}, Role: {p['role_status']}" == status_combo][:3]
        print(f"\n{status_combo}:")
        for ex in examples:
            print(f"- {ex['name']} ({ex['ods_code']})")
            print(f"  Operational: {ex['operational_start']} to {ex['operational_end'] or 'present'}")
            print(f"  Legal: {ex['legal_start']} to {ex['legal_end'] or 'present'}")
    
    # Look for specific examples
    print("\nLooking for THE REGENTS PARK PRACTICE:")
    regents = next((p for p in practices if p['name'] == "THE REGENTS PARK PRACTICE"), None)
    if regents:
        print(f"ODS Code: {regents['ods_code']}")
        print(f"Organisation Status: {regents['org_status']}")
        print(f"Role Status: {regents['role_status']}")
        print(f"Operational: {regents['operational_start']} to {regents['operational_end'] or 'present'}")
        print(f"Legal: {regents['legal_start']} to {regents['legal_end'] or 'present'}")

def examine_specific_practices(data):
    """Examine specific practices in detail"""
    print("\nExamining Specific Practices:")
    print("============================")
    
    target_practices = ["THE REGENTS PARK PRACTICE"]
    
    for ods_code, org_data in data["organisations"].items():
        org_info = org_data.get("Organisation", {})
        name = org_info.get("Name")
        
        if name in target_practices:
            print(f"\nFound practice: {name}")
            print("Raw data:")
            print(json.dumps(org_info, indent=2))

def analyze_practice_relationships(data):
    """Analyze practice relationships in detail"""
    print("\nAnalyzing Practice Relationships:")
    print("==============================")
    
    practices = []
    for ods_code, org_data in data["organisations"].items():
        org_info = org_data.get("Organisation", {})
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        if any(role.get("id") == "RO76" for role in roles):
            # Get all relationships
            rels = org_info.get("Rels", {}).get("Rel", [])
            if not isinstance(rels, list):
                rels = [rels]
            
            # Group relationships by type
            rel_by_type = defaultdict(list)
            for rel in rels:
                rel_by_type[rel.get("id")].append({
                    'target_ods': rel.get("Target", {}).get("OrgId", {}).get("extension"),
                    'status': rel.get("Status"),
                    'dates': rel.get("Date", []),
                    'target_role': rel.get("Target", {}).get("PrimaryRoleId", {}).get("id")
                })
            
            practices.append({
                'ods_code': ods_code,
                'name': org_info.get("Name"),
                'status': org_info.get("Status"),
                'relationships': dict(rel_by_type)
            })
    
    # Look for patterns in relationships
    print("\nAnalyzing relationship patterns:")
    for practice in practices:
        # Check if this is a known inactive practice
        if practice['name'] == "THE REGENTS PARK PRACTICE":
            print(f"\nDetailed analysis of {practice['name']} ({practice['ods_code']}):")
            print(f"Status in ODS: {practice['status']}")
            print("\nRelationships:")
            for rel_type, rels in practice['relationships'].items():
                print(f"\n{rel_type}:")
                for rel in rels:
                    dates = rel['dates'] if isinstance(rel['dates'], list) else [rel['dates']]
                    for date in dates:
                        print(f"  Target: {rel['target_ods']} ({rel['target_role']})")
                        print(f"  Status: {rel['status']}")
                        print(f"  Start: {date.get('Start')}")
                        print(f"  End: {date.get('End', 'present')}")

def main():
    icb_code = "93C"  # North Central London ICB
    
    # Load or fetch the data
    data = load_or_fetch_data(icb_code)
    
    # Analyze practice relationships
    analyze_practice_relationships(data)
    
    # Create practice/PCN report
    timestamp = datetime.now().strftime("%Y%m%d")
    report_filename = f"ncl_icb_practices_pcns_{timestamp}.xlsx"
    create_practice_pcn_report(data, report_filename)

if __name__ == "__main__":
    main()
