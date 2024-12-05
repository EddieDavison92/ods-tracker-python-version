import logging
import os
from datetime import datetime

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create a timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d")
    
    # Setup logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/changes_{timestamp}.log'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def validate_org_data(org_data):
    """Validate organization data structure"""
    required_fields = ["Name", "Status", "OrgId", "Roles"]
    missing_fields = [field for field in required_fields if field not in org_data]
    
    if missing_fields:
        logging.warning(f"Missing required fields in organization data: {missing_fields}")
        return False
    return True

def get_pcn_name(data, pcn_ods):
    """Get PCN name from ODS code"""
    if not pcn_ods:
        return None
        
    org_data = data.get("organisations", {}).get(pcn_ods, {}).get("Organisation", {})
    return org_data.get("Name") 