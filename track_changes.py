import json
import os
from datetime import datetime
from glob import glob
from utils import setup_logging, get_pcn_name

logger = setup_logging()

def load_latest_data():
    """Load the most recent data file"""
    try:
        data_files = glob('data/ncl_icb_data_*.json')
        if not data_files:
            logger.warning("No data files found in data directory")
            return None, None
        
        latest_file = max(data_files)
        with open(latest_file, 'r') as f:
            return json.load(f), latest_file
    except Exception as e:
        logger.error(f"Error loading latest data: {e}")
        return None, None

def load_previous_data(current_file):
    """Load the previous data file"""
    try:
        data_files = glob('data/ncl_icb_data_*.json')
        if len(data_files) < 2:
            logger.warning("No previous data file found")
            return None, None
        
        # Sort files and get the second most recent
        sorted_files = sorted(data_files)
        prev_file_idx = sorted_files.index(current_file) - 1
        if prev_file_idx < 0:
            logger.warning("No previous file found")
            return None, None
        
        with open(sorted_files[prev_file_idx], 'r') as f:
            return json.load(f), sorted_files[prev_file_idx]
    except Exception as e:
        logger.error(f"Error loading previous data: {e}")
        return None, None

def detect_practice_changes(old_data, new_data):
    """Detect changes in GP Practices"""
    changes = []
    
    old_orgs = old_data.get("organisations", {})
    new_orgs = new_data.get("organisations", {})
    
    # Check all practices in both old and new data
    all_ods_codes = set(old_orgs.keys()) | set(new_orgs.keys())
    
    for ods_code in all_ods_codes:
        old_org = old_orgs.get(ods_code, {}).get("Organisation", {})
        new_org = new_orgs.get(ods_code, {}).get("Organisation", {})
        
        # Skip if not a practice
        if not any(role.get("id") == "RO76" for role in new_org.get("Roles", {}).get("Role", []) or []):
            continue
        
        # New practice
        if not old_org:
            changes.append({
                "type": "new_practice",
                "ods_code": ods_code,
                "name": new_org.get("Name"),
                "date_of_change": new_org.get("LastChangeDate")
            })
            continue
            
        # Closed practice
        if not new_org:
            changes.append({
                "type": "closed_practice",
                "ods_code": ods_code,
                "name": old_org.get("Name"),
                "date_of_change": old_org.get("LastChangeDate")
            })
            continue
        
        # Status change
        if old_org.get("Status") != new_org.get("Status"):
            changes.append({
                "type": "status_change",
                "ods_code": ods_code,
                "name": new_org.get("Name"),
                "old_status": old_org.get("Status"),
                "new_status": new_org.get("Status"),
                "date_of_change": new_org.get("LastChangeDate")
            })
        
        # PCN membership change
        old_pcn = get_current_pcn(old_org)
        new_pcn = get_current_pcn(new_org)
        
        if old_pcn != new_pcn:
            changes.append({
                "type": "pcn_change",
                "ods_code": ods_code,
                "name": new_org.get("Name"),
                "old_pcn": old_pcn,
                "old_pcn_name": get_pcn_name(old_data, old_pcn),
                "new_pcn": new_pcn,
                "new_pcn_name": get_pcn_name(new_data, new_pcn),
                "date_of_change": new_org.get("LastChangeDate")
            })
    
    return changes

def get_current_pcn(org_data):
    """Get current PCN for a practice"""
    try:
        rels = org_data.get("Rels", {}).get("Rel", [])
        if not isinstance(rels, list):
            rels = [rels]
        
        for rel in rels:
            if (rel.get("Target", {}).get("PrimaryRoleId", {}).get("id") == "RO272" and
                rel.get("Status") == "Active" and
                not any(d.get("End") for d in rel.get("Date", []))):
                return rel.get("Target", {}).get("OrgId", {}).get("extension")
        return None
    except Exception as e:
        logger.error(f"Error getting current PCN: {e}")
        return None

def detect_pcn_changes(old_data, new_data):
    """Detect changes in PCNs"""
    changes = []
    
    old_orgs = old_data.get("organisations", {})
    new_orgs = new_data.get("organisations", {})
    
    # Check all PCNs in both old and new data
    all_ods_codes = set(old_orgs.keys()) | set(new_orgs.keys())
    
    for ods_code in all_ods_codes:
        old_org = old_orgs.get(ods_code, {}).get("Organisation", {})
        new_org = new_orgs.get(ods_code, {}).get("Organisation", {})
        
        # Skip if not a PCN
        if not any(role.get("id") == "RO272" and role.get("primaryRole", False) 
                  for role in new_org.get("Roles", {}).get("Role", []) or []):
            continue
        
        # New PCN
        if not old_org:
            changes.append({
                "type": "new_pcn",
                "ods_code": ods_code,
                "name": new_org.get("Name"),
                "date_of_change": new_org.get("LastChangeDate")
            })
            continue
            
        # Closed PCN
        if not new_org:
            changes.append({
                "type": "closed_pcn",
                "ods_code": ods_code,
                "name": old_org.get("Name"),
                "date_of_change": old_org.get("LastChangeDate")
            })
            continue
        
        # Status change
        if old_org.get("Status") != new_org.get("Status"):
            changes.append({
                "type": "status_change",
                "ods_code": ods_code,
                "name": new_org.get("Name"),
                "old_status": old_org.get("Status"),
                "new_status": new_org.get("Status"),
                "date_of_change": new_org.get("LastChangeDate")
            })
    
    return changes

def update_tracked_changes(changes, current_file):
    """Update the tracked_changes.json file"""
    try:
        tracked_changes_path = 'data/tracked_changes.json'
        
        if os.path.exists(tracked_changes_path):
            with open(tracked_changes_path, 'r') as f:
                tracked_changes = json.load(f)
        else:
            tracked_changes = {"changes": []}
        
        # Add new changes with summary statistics
        if changes["practice_changes"] or changes["pcn_changes"]:
            summary = {
                "total_changes": len(changes["practice_changes"]) + len(changes["pcn_changes"]),
                "practice_changes": {
                    "total": len(changes["practice_changes"]),
                    "new": len([c for c in changes["practice_changes"] if c["type"] == "new_practice"]),
                    "closed": len([c for c in changes["practice_changes"] if c["type"] == "closed_practice"]),
                    "status": len([c for c in changes["practice_changes"] if c["type"] == "status_change"]),
                    "pcn": len([c for c in changes["practice_changes"] if c["type"] == "pcn_change"])
                },
                "pcn_changes": {
                    "total": len(changes["pcn_changes"]),
                    "new": len([c for c in changes["pcn_changes"] if c["type"] == "new_pcn"]),
                    "closed": len([c for c in changes["pcn_changes"] if c["type"] == "closed_pcn"]),
                    "status": len([c for c in changes["pcn_changes"] if c["type"] == "status_change"])
                }
            }
            
            tracked_changes["changes"].append({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "data_file": os.path.basename(current_file),
                "summary": summary,
                "practice_changes": changes["practice_changes"],
                "pcn_changes": changes["pcn_changes"]
            })
            
            # Save updated changes
            with open(tracked_changes_path, 'w') as f:
                json.dump(tracked_changes, f, indent=2)
            logger.info(f"Changes saved to {tracked_changes_path}")
            
            # Log summary
            logger.info("Change Summary:")
            logger.info(f"Total changes: {summary['total_changes']}")
            logger.info(f"Practice changes: {summary['practice_changes']}")
            logger.info(f"PCN changes: {summary['pcn_changes']}")
        else:
            logger.info("No changes detected")
            
    except Exception as e:
        logger.error(f"Error updating tracked changes: {e}")

def main():
    try:
        # Load latest data
        latest_data, latest_file = load_latest_data()
        if not latest_data:
            return
        
        # Load previous data
        previous_data, previous_file = load_previous_data(latest_file)
        if not previous_data:
            return
        
        logger.info(f"Comparing {os.path.basename(previous_file)} with {os.path.basename(latest_file)}")
        
        # Detect changes
        practice_changes = detect_practice_changes(previous_data, latest_data)
        pcn_changes = detect_pcn_changes(previous_data, latest_data)
        
        changes = {
            "practice_changes": practice_changes,
            "pcn_changes": pcn_changes
        }
        
        # Update tracked changes file
        update_tracked_changes(changes, latest_file)
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")

if __name__ == "__main__":
    main() 