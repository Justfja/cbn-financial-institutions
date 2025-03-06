import logging
import pandas as pd
import os
import glob
from datetime import datetime
import json


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
    current_df = pd.read_csv(current_file)
    previous_df = pd.read_csv(previous_file)
    
    # Create unique identifier combining name and category
    current_df['identifier'] = current_df['Institution Name'] + ' (' + current_df['Category'] + ')'
    previous_df['identifier'] = previous_df['Institution Name'] + ' (' + previous_df['Category'] + ')'
    
    # Get institution identifiers
    current_identifiers = set(current_df['identifier'])
    previous_identifiers = set(previous_df['identifier'])
    
    # Find additions and removals with category info
    added_identifiers = current_identifiers - previous_identifiers
    removed_identifiers = previous_identifiers - current_identifiers
    
    # Get full information about added and removed institutions
    added = []
    for identifier in added_identifiers:
        inst_row = current_df[current_df['identifier'] == identifier].iloc[0]
        added.append({
            'name': inst_row['Institution Name'],
            'category': inst_row['Category'],
            'identifier': identifier,
            'website': inst_row.get('Website', 'N/A') if pd.notna(inst_row.get('Website', 'N/A')) else 'N/A',
            'address': inst_row.get('Street Address', 'N/A') if pd.notna(inst_row.get('Street Address', 'N/A')) else 'N/A'
        })
    
    removed = []
    for identifier in removed_identifiers:
        inst_row = previous_df[previous_df['identifier'] == identifier].iloc[0]
        removed.append({
            'name': inst_row['Institution Name'],
            'category': inst_row['Category'],
            'identifier': identifier,
            'website': inst_row.get('Website', 'N/A') if pd.notna(inst_row.get('Website', 'N/A')) else 'N/A',
            'address': inst_row.get('Street Address', 'N/A') if pd.notna(inst_row.get('Street Address', 'N/A')) else 'N/A'
        })
    
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
        'added': added,
        'removed': removed,
        'changed': changed,
        'total_current': len(current_identifiers),
        'total_previous': len(previous_identifiers)
    }



def generate_change_report(changes):
    """
    Generate a human-readable change report
    """
    today = datetime.now().strftime('%Y-%m-%d')
    
    # Make sure report directory exists
    os.makedirs('data/report', exist_ok=True)
    
    with open(f'data/report/changes_{today}.txt', 'w') as f:
        f.write(f"CBN Financial Institutions Change Report - {today}\n")
        f.write("="*80 + "\n\n")
        
        has_changes = bool(changes['added'] or changes['removed'] or changes['changed'])
        
        # Save change data for email implementation
        change_data = {
            'has_changes': has_changes,
            'new_institutions': len(changes['added']),
            'removed_institutions': len(changes['removed']),
            'changed_institutions': len(set((c['Institution'], c['Category']) for c in changes['changed'])),
            'report_date': today
        }
        
        # Save the change data to a JSON file
        with open(f'data/change_data_{today}.json', 'w') as json_file:
            json.dump(change_data, json_file)
        
        # Create a symlink or copy to "latest" for easier access
        latest_path = 'data/change_data_latest.json'
        if os.path.exists(latest_path):
            os.remove(latest_path)
        with open(latest_path, 'w') as latest_file:
            json.dump(change_data, latest_file)
        
        if not has_changes:
            f.write("No changes detected between current and previous data.\n")
            return
        
        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total institutions: {changes['total_current']} (previous: {changes['total_previous']})\n")
        f.write(f"New institutions: {len(changes['added'])}\n")
        f.write(f"Removed institutions: {len(changes['removed'])}\n")
        f.write(f"Institutions with changed details: {len(set((c['Institution'], c['Category']) for c in changes['changed']))}\n\n")
        
        if changes['added']:
            f.write("ADDED INSTITUTIONS\n")
            f.write("-" * 80 + "\n")
            
            # Group additions by category
            added_by_category = {}
            for inst in changes['added']:
                if inst['category'] not in added_by_category:
                    added_by_category[inst['category']] = []
                added_by_category[inst['category']].append(inst)
            
            # List additions by category
            for category, institutions in sorted(added_by_category.items()):
                f.write(f"\n[{category}]\n")
                for inst in sorted(institutions, key=lambda x: x['name']):
                    f.write(f"• {inst['name']}\n")
                    contact_info = []
                    if inst['website'] != 'N/A':
                        contact_info.append(f"Website: {inst['website']}")
                    if inst['address'] != 'N/A':
                        contact_info.append(f"Address: {inst['address']}")
                    
                    if contact_info:
                        f.write(f"  {' | '.join(contact_info)}\n")
            f.write("\n")
            
        if changes['removed']:
            f.write("REMOVED INSTITUTIONS\n")
            f.write("-" * 80 + "\n")
            
            # Group removals by category
            removed_by_category = {}
            for inst in changes['removed']:
                if inst['category'] not in removed_by_category:
                    removed_by_category[inst['category']] = []
                removed_by_category[inst['category']].append(inst)
            
            # List removals by category
            for category, institutions in sorted(removed_by_category.items()):
                f.write(f"\n[{category}]\n")
                for inst in sorted(institutions, key=lambda x: x['name']):
                    f.write(f"• {inst['name']}\n")
                    contact_info = []
                    if inst['website'] != 'N/A':
                        contact_info.append(f"Website: {inst['website']}")
                    if inst['address'] != 'N/A':
                        contact_info.append(f"Address: {inst['address']}")
                    
                    if contact_info:
                        f.write(f"  {' | '.join(contact_info)}\n")
            f.write("\n")
            
        if changes['changed']:
            f.write("CHANGED INSTITUTION DETAILS\n")
            f.write("-" * 80 + "\n\n")
            
            # Group changes by category and institution
            changes_by_category = {}
            for change in changes['changed']:
                cat = change['Category']
                if cat not in changes_by_category:
                    changes_by_category[cat] = {}
                
                inst = change['Institution']
                if inst not in changes_by_category[cat]:
                    changes_by_category[cat][inst] = []
                
                changes_by_category[cat][inst].append(change)
            
            # List changes by category and institution
            for category in sorted(changes_by_category.keys()):
                f.write(f"[{category}]\n")
                
                for inst in sorted(changes_by_category[category].keys()):
                    f.write(f"• {inst}\n")
                    
                    for change in changes_by_category[category][inst]:
                        f.write(f"  - {change['Field']}: '{change['Previous']}' → '{change['Current']}'\n")
                    
                    f.write("\n")
    

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
        
        # Make sure report directory exists
        os.makedirs('data/report', exist_ok=True)
        
        # Create special baseline report
        with open(f'data/report/initial_baseline_{today}.txt', 'w') as f:
            f.write(f"CBN Financial Institutions Initial Baseline - {today}\n")
            f.write("="*80 + "\n\n")
            f.write(f"Total institutions: {len(current_df)}\n\n")
            
            # Group by category
            category_groups = current_df.groupby('Category')
            
            for category, group in sorted(category_groups):
                f.write(f"[{category}] - {len(group)} institutions\n")
                f.write("-" * 80 + "\n")
                
                for _, row in group.sort_values('Institution Name').iterrows():
                    f.write(f"• {row['Institution Name']}\n")
                    
                    contact_info = []
                    if 'Website' in row and pd.notna(row['Website']):
                        contact_info.append(f"Website: {row['Website']}")
                    if 'Street Address' in row and pd.notna(row['Street Address']):
                        contact_info.append(f"Address: {row['Street Address']}")
                    
                    if contact_info:
                        f.write(f"  {' | '.join(contact_info)}\n")
                
                f.write("\n")
        
        # Store data for first run
        change_data = {
            'has_changes': False,  # No changes for baseline
            'new_institutions': len(current_df),  # All considered new for baseline
            'removed_institutions': 0,
            'changed_institutions': 0,
            'report_date': today,
            'is_baseline': True
        }
        
        # Save the change data to a JSON file
        with open(f'data/change_data_{today}.json', 'w') as json_file:
            json.dump(change_data, json_file)
        
        # Create a symlink or copy to "latest" for easier access
        latest_path = 'data/change_data_latest.json'
        if os.path.exists(latest_path):
            os.remove(latest_path)
        with open(latest_path, 'w') as latest_file:
            json.dump(change_data, latest_file)
        
        logger.info(f"Created initial baseline report with {len(current_df)} institutions.")
        return
    
    # Compare the data
    changes = compare_institutions(current_file, previous_file)
    
    # Generate change report
    generate_change_report(changes)
    
    # Create empty file if no changes (for backward compatibility)
    if not changes['added'] and not changes['removed'] and not changes['changed']:
        today = datetime.now().strftime('%Y-%m-%d')
        with open(f'data/report/no_changes_{today}.txt', 'w') as f:
            pass
    
    # Cleanup old files (optional)
    cleanup_old_files(keep_latest=3)



if __name__ == "__main__":
    main()