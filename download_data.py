import requests
import json
from datetime import datetime
import os
import pandas as pd
from tqdm import tqdm
from utils import setup_logging, validate_org_data

logger = setup_logging()

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
            "RelTypeId": "RE4,RE6",  # IS COMMISSIONED BY, IS OPERATED BY
            "TargetOrgId": ods_code,
            "_format": "json"
        }
        
        try:
            response = requests.get(current_url, params=params, headers=self.headers)
            response.raise_for_status()
            total_count = int(response.headers.get('X-Total-Count', 0))
            
            logger.info(f"Found {total_count} organisations to process")
            
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
            logger.error(f"Error getting related organisations: {e}")
            if 'response' in locals():
                logger.error(f"Response status code: {response.status_code}")
                logger.error(f"Response headers: {response.headers}")
                logger.error(f"Response content: {response.text[:500]}...")
        finally:
            if 'pbar' in locals():
                pbar.close()
        
        return all_orgs

    def get_org_details(self, ods_code):
        """Get full details for a specific organization"""
        url = f"{self.base_url}/organisations/{ods_code}"
        params = {"_format": "json"}
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            
            # Validate organization data
            if not validate_org_data(data.get("Organisation", {})):
                logger.warning(f"Invalid organization data for {ods_code}")
            
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting organisation details for {ods_code}: {e}")
            return None

def save_raw_data(data, filename):
    """Save raw data to JSON file"""
    try:
        os.makedirs("data", exist_ok=True)
        filepath = os.path.join("data", filename)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Raw data saved to {filepath}")
    except Exception as e:
        logger.error(f"Error saving raw data: {e}")

def extract_practice_data(org_data):
    """Extract relevant GP Practice data"""
    try:
        org_info = org_data.get("Organisation", {})
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        if not any(role.get("id") == "RO76" for role in roles):
            return None
            
        # Get dates
        dates = org_info.get("Date", [])
        if not isinstance(dates, list):
            dates = [dates]
        
        # Get PCN relationship
        rels = org_info.get("Rels", {}).get("Rel", [])
        if not isinstance(rels, list):
            rels = [rels]
        
        pcn_rel = next((rel for rel in rels 
                        if rel.get("Target", {}).get("PrimaryRoleId", {}).get("id") == "RO272"
                        and rel.get("Status") == "Active"
                        and not any(d.get("End") for d in rel.get("Date", []))), None)
        
        location = org_info.get("GeoLoc", {}).get("Location", {})
        
        return {
            'ODS Code': org_info.get("OrgId", {}).get("extension"),
            'Name': org_info.get("Name"),
            'Status': org_info.get("Status"),
            'Operational Start': next((d.get("Start") for d in dates if d.get("Type") == "Operational"), None),
            'Operational End': next((d.get("End") for d in dates if d.get("Type") == "Operational"), None),
            'Address': location.get("AddrLn1"),
            'Town': location.get("Town"),
            'Postcode': location.get("PostCode"),
            'Current PCN Code': pcn_rel.get("Target", {}).get("OrgId", {}).get("extension") if pcn_rel else None,
            'Last Changed': org_info.get("LastChangeDate")
        }
    except Exception as e:
        logger.error(f"Error extracting practice data: {e}")
        return None

def extract_pcn_data(org_data):
    """Extract relevant PCN data"""
    try:
        org_info = org_data.get("Organisation", {})
        roles = org_info.get("Roles", {}).get("Role", [])
        if not isinstance(roles, list):
            roles = [roles]
        
        if not any(role.get("id") == "RO272" and role.get("primaryRole", False) for role in roles):
            return None
        
        location = org_info.get("GeoLoc", {}).get("Location", {})
        
        return {
            'ODS Code': org_info.get("OrgId", {}).get("extension"),
            'Name': org_info.get("Name"),
            'Status': org_info.get("Status"),
            'Address': location.get("AddrLn1"),
            'Town': location.get("Town"),
            'Postcode': location.get("PostCode"),
            'Last Changed': org_info.get("LastChangeDate")
        }
    except Exception as e:
        logger.error(f"Error extracting PCN data: {e}")
        return None

def main():
    try:
        # Initialize the ODS client
        client = ODSClient()
        
        # North Central London ICB ODS code
        icb_code = "93C"
        
        # Get all related organizations
        related_orgs = client.get_related_orgs(icb_code)
        logger.info(f"Found {len(related_orgs)} organisations")
        
        # Get full details for each organization
        full_data = {
            "metadata": {
                "icb_code": icb_code,
                "download_date": datetime.now().isoformat(),
                "total_organisations": len(related_orgs)
            },
            "organisations": {}
        }
        
        practices_data = []
        pcns_data = []
        
        logger.info("Processing organisations...")
        for org in tqdm(related_orgs):
            ods_code = org.get("OrgId")
            if ods_code:
                org_details = client.get_org_details(ods_code)
                if org_details:
                    full_data["organisations"][ods_code] = org_details
                    
                    # Extract practice data if applicable
                    practice_data = extract_practice_data(org_details)
                    if practice_data:
                        practices_data.append(practice_data)
                    
                    # Extract PCN data if applicable
                    pcn_data = extract_pcn_data(org_details)
                    if pcn_data:
                        pcns_data.append(pcn_data)
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d")
        json_filename = f"ncl_icb_data_{timestamp}.json"
        save_raw_data(full_data, json_filename)
        
        # Save CSVs
        os.makedirs("data", exist_ok=True)
        
        if practices_data:
            practices_df = pd.DataFrame(practices_data)
            practices_df.sort_values(['Status', 'Name'], inplace=True)
            practices_df.to_csv('data/practices.csv', index=False)
            logger.info(f"Saved {len(practices_data)} practices to practices.csv")
        
        if pcns_data:
            pcns_df = pd.DataFrame(pcns_data)
            pcns_df.sort_values('Name', inplace=True)
            pcns_df.to_csv('data/pcns.csv', index=False)
            logger.info(f"Saved {len(pcns_data)} PCNs to pcns.csv")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main() 