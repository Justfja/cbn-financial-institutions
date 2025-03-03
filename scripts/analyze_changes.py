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
            logging.StreamHandler()  # Also output to console               +++++++++++++++
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
    current_df = pd.read_csv(current_file)
    previous_df = pd.read_csv(previous_file)
    
    # Create unique identifier combining name and category
    current_df['identifier'] = current_df['Institution Name'] + ' (' + current_df['Category'] + ')'
    previous_df['identifier'] = previous_df['Institution Name'] + ' (' + previous_df['Category'] + ')'
    
    # Get institution identifiers
    current_identifiers = set(current_df['identifier'])
    previous_identifiers = set(previous_df['identifier'])
    
    # Find additions and removals with category info
    added = current_identifiers - previous_identifiers
    removed = previous_identifiers - current_identifiers
    
    # Check for name changes and other field changes
    changed = []
    for index, row in current_df.iterrows():
        inst_name = row['Institution Name']
        category = row['Category']
        identifier = row['identifier']
        
        # Look for same institution (by name and category)
        if identifier in previous_identifiers:
            # Get previous record
            prev_row = previous_df[previous_df['identifier'] == identifier].iloc[0]
            
            # Check for changes in key fields
            for field in ['Street Address', 'Website', 'Telephone number']:
                if field in row and field in prev_row:
                    if str(row[field]) != str(prev_row[field]):
                        changed.append({
                            'Institution': inst_name,
                            'Category': category,
                            'Field': field,
                            'Previous': prev_row[field],
                            'Current': row[field]
                        })
    
    return {
        'added': list(added),
        'removed': list(removed),
        'changed': changed,
        'total_current': len(current_identifiers),
        'total_previous': len(previous_identifiers)
    }


def generate_change_report(changes):
    """
    Generate a human-readable change report
    """
    today = datetime.now().strftime('%Y-%m-%d')
    
    with open(f'data/report/changes_{today}.txt', 'w') as f:
        f.write(f"CBN Financial Institutions Change Report - {today}\n")
        f.write("="*60 + "\n\n")
        
        if not changes['added'] and not changes['removed'] and not changes['changed']:
            f.write("No changes detected between current and previous data.\n")
            return
        
        f.write(f"SUMMARY:\n")
        f.write(f"Total institutions: {changes['total_current']} (previous: {changes['total_previous']})\n")
        f.write(f"New institutions: {len(changes['added'])}\n")
        f.write(f"Removed institutions: {len(changes['removed'])}\n")
        f.write(f"Institutions with changed details: {len(set((c['Institution'], c['Category']) for c in changes['changed']))}\n\n")
        
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
            current_key = None
            for change in sorted(changes['changed'], key=lambda x: (x['Category'], x['Institution'])):
                key = (change['Institution'], change['Category'])
                if current_key != key:
                    current_key = key
                    f.write(f"\n* {change['Institution']} ({change['Category']}):\n")
                f.write(f"  - {change['Field']}: '{change['Previous']}' → '{change['Current']}'\n")

    

def cleanup_old_files(keep_latest=3):
    """
    Optionally cleanup older data files, keeping the most recent ones
    """
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
        # Create an initial baseline report instead of a comparison
        today = datetime.now().strftime('%Y-%m-%d')
        current_df = pd.read_csv(current_file)
        
        with open(f'data/report/initial_baseline_{today}.txt', 'w') as f:
            f.write(f"CBN Financial Institutions Initial Baseline - {today}\n")
            f.write("="*60 + "\n\n")
            f.write(f"Total institutions: {len(current_df)}\n\n")
            f.write("INSTITUTIONS:\n")
            
            # List all institutions in the baseline
            for name in sorted(current_df['Institution Name']):
                f.write(f"- {name}\n")
        
        logger.info(f"Created initial baseline report with {len(current_df)} institutions.")
        return
    
    # Compare the data
    changes = compare_institutions(current_file, previous_file)
    
    # Generate change report
    generate_change_report(changes)
    
    # Create empty file if no changes
    if not changes['added'] and not changes['removed'] and not changes['changed']:
        today = datetime.now().strftime('%Y-%m-%d')
        with open(f'data/report/no_changes_{today}.txt', 'w') as f:
            pass
    
    # Cleanup old files (optional)
    cleanup_old_files(keep_latest=3)



if __name__ == "__main__":
    main()