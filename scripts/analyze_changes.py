import logging
import pandas as pd
import os
import glob
from datetime import datetime


## add to log files
def setup_add_logging():
    # Create logs directory if it doesn't exist
    os.makedirs('data/logs', exist_ok=True)
    
    # Get current date for log filename
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'data/logs/cbn_analysis_{today}.log'),
            logging.StreamHandler()  # Also output to console
        ]
    )
    
    return logging.getLogger()



## Set up logger
logger = setup_add_logging()



def get_latest_and_previous_files():
    """
    Get the paths to the latest and previous data files
    """

    # Get all dated files
    files = glob.glob('data/cbn_data/cbn_all_financial_institutions_*.csv')
    files = [f for f in files if not f.endswith('_latest.csv')]
    
    # Sort by date (newest first)
    files.sort(reverse=True)
    
    if len(files) <= 1:
        return files[0] if files else None, None
    
    return files[0], files[1]



def compare_institutions(current_file, previous_file):
    """
    Compare current and previous institution data
    """

    # Load data
    try:
        current_df = pd.read_csv(current_file)
        previous_df = pd.read_csv(previous_file)
    except Exception as e:
        logger.error(f"Could not read current and/or previous scrapped data: {str(e)}")
    
    try:
        # Get institution names
        current_names = set(current_df['Institution Name'])
        previous_names = set(previous_df['Institution Name'])
        
        # Find additions and removals
        added = current_names - previous_names
        removed = previous_names - current_names
        
        # Check for name changes (This can be more sophisticated to capture other patterns)
        changed = []
        for index, row in current_df.iterrows():
            inst_name = row['Institution Name']
            if inst_name in previous_names:
                # Get previous record
                prev_row = previous_df[previous_df['Institution Name'] == inst_name].iloc[0]
                
                # Check for changes in key fields
                for field in ['Website']: #, 'Telephone number' 'Street Address', 
                    if field in row and field in prev_row:
                        if str(row[field]) != str(prev_row[field]):
                            changed.append({
                                'Institution': inst_name,
                                'Field': field,
                                'Previous': prev_row[field],
                                'Current': row[field]
                            })
        logger.info("Successfully Checked data")
        return {
            'added': list(added),
            'removed': list(removed),
            'changed': changed,
            'total_current': len(current_names),
            'total_previous': len(previous_names)
        }
    except Exception as e:
        logger.error(f"Error occured during analysis and checks: {str(e)}")
        return {
            'added': [],
            'removed': [],
            'changed': [],
            'total_current': [],
            'total_previous': []
        }
    
    



def generate_change_report(changes):
    """
    Generate a human-readable change report
    """

    today = datetime.now().strftime('%Y-%m-%d')
    
    with open(f'data/cbn_data/changes_{today}.txt', 'w') as f:
        f.write(f"CBN Financial Institutions Change Report - {today}\n")
        f.write("="*60 + "\n\n")
        
        if not changes['added'] and not changes['removed'] and not changes['changed']:
            f.write("No changes detected between current and previous data.\n")
            return
        
        f.write(f"SUMMARY:\n")
        f.write(f"Total institutions: {changes['total_current']} (previous: {changes['total_previous']})\n")
        f.write(f"New institutions: {len(changes['added'])}\n")
        f.write(f"Removed institutions: {len(changes['removed'])}\n")
        f.write(f"Institutions with changed details: {len(set(c['Institution'] for c in changes['changed']))}\n\n")
        
        if changes['added']:
            f.write("ADDED INSTITUTIONS:\n")
            for inst in sorted(changes['added']):
                f.write(f"+ {inst}\n")
            f.write("\n")
            
        if changes['removed']:
            f.write("REMOVED INSTITUTIONS:\n")
            for inst in sorted(changes['removed']):
                f.write(f"- {inst}\n")
            f.write("\n")
            
        if changes['changed']:
            f.write("CHANGED INSTITUTION DETAILS:\n")
            current_inst = None
            for change in sorted(changes['changed'], key=lambda x: x['Institution']):
                if current_inst != change['Institution']:
                    current_inst = change['Institution']
                    f.write(f"\n* {current_inst}:\n")
                f.write(f"  - {change['Field']}: '{change['Previous']}' → '{change['Current']}'\n")


def cleanup_old_files(keep_latest=3):
    """Optionally cleanup older data files, keeping the most recent ones"""
    files = glob.glob('data/cbn_data/cbn_all_financial_institutions_*.csv')
    files = [f for f in files if not f.endswith('_latest.csv')]
    
    if len(files) <= keep_latest:
        return
        
    # Sort by date (oldest first)
    files.sort()
    
    # Remove older files
    for file in files[:-keep_latest]:
        try:
            os.remove(file)
            logger.info(f"Removed old data file: {file}")
        except Exception as e:
            logger.error(f"Error removing {file}: {str(e)}")



def main():
    # Get the latest and previous data files
    current_file, previous_file = get_latest_and_previous_files()
    
    if not current_file:
        logger.error("No data files found.")
        return
        
    if not previous_file:
        logger.error("Only one data file found. No comparison possible.")
        return
    
    # Compare the data
    changes = compare_institutions(current_file, previous_file)
    
    # Generate change report
    generate_change_report(changes)
    
    # Create empty file if no changes
    if not changes['added'] and not changes['removed'] and not changes['changed']:
        today = datetime.now().strftime('%Y-%m-%d')
        with open(f'data/cbn_data/no_changes_{today}.txt', 'w') as f:
            pass
    
    # Cleanup old files (optional)
    cleanup_old_files(keep_latest=3)



if __name__ == "__main__":
    main()