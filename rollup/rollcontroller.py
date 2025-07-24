import json
import copy
from datetime import datetime
from typing import Dict, List, Any, Optional

class SiteDataRollup:
    def __init__(self):
        self.new_rollup_table = []  # This will be our new table with rolled up data
        self.rollup_contributions = {}  # Track contributions from children to parents
    
    def find_cdata_for_site(self, cdata_list: List[Dict], site_code: str, year: int, internal_code_id: str) -> List[Dict]:
        """
        Find all cdata entries for a specific site, year, and internal_code_id
        """
        matching_cdata = []
        for cdata in cdata_list:
            # Match site_code, year, and internal_code_id
            cdata_year = cdata.get('type_year') or cdata.get('reporting_year')
            cdata_internal_id = str(cdata.get('internal_code_id', {}).get('$oid', ''))
            
            if (cdata.get('site_code') == site_code and 
                cdata_year == year and 
                cdata_internal_id == internal_code_id):
                matching_cdata.append(cdata)
        return matching_cdata
    
    def create_rollup_record(self, original_cdata: Dict, site: Dict, rollup_qty: float = 0, rollup_value: float = 0) -> Dict:
        """
        Create a new record for the rollup table
        """
        new_record = copy.deepcopy(original_cdata)
        
        # Add rollup fields
        new_record['rollup_qty'] = rollup_qty
        new_record['rollup_value'] = rollup_value
        new_record['site_ownership'] = site.get('ownership', 100)
        new_record['site_id'] = site['id']
        new_record['created_at'] = {'$date': datetime.now().isoformat()}
        new_record['rollup_processed'] = True
        
        return new_record
    
    def get_site_by_code(self, site_data: Dict, target_code: str) -> Optional[Dict]:
        """
        Recursively find a site by its internal_site_code
        """
        if site_data.get('internal_site_code') == target_code:
            return site_data
        
        if 'sites' in site_data:
            for child_site in site_data['sites']:
                result = self.get_site_by_code(child_site, target_code)
                if result:
                    return result
        return None
    
    def get_parent_site(self, site_data: Dict, target_code: str, parent: Dict = None) -> Optional[Dict]:
        """
        Find the parent site of a given site code
        """
        if site_data.get('internal_site_code') == target_code:
            return parent
        
        if 'sites' in site_data:
            for child_site in site_data['sites']:
                result = self.get_parent_site(child_site, target_code, site_data)
                if result:
                    return result
        return None
    
    def get_all_leaf_sites(self, site_data: Dict, leaf_sites: List = None) -> List[Dict]:
        """
        Get all leaf sites (sites with no children)
        """
        if leaf_sites is None:
            leaf_sites = []
        
        # If this site has no children or empty children, it's a leaf
        if not site_data.get('sites') or len(site_data['sites']) == 0:
            leaf_sites.append(site_data)
        else:
            # Recursively check children
            for child_site in site_data['sites']:
                self.get_all_leaf_sites(child_site, leaf_sites)
        
        return leaf_sites
    
    def process_site_rollup(self, site_data: Dict, cdata_list: List[Dict], year: int, internal_code_id: str):
        """
        Main rollup process: start from leaf sites and go up
        """
        print(f"Starting rollup process for year {year} and internal_code_id {internal_code_id}")
        print("="*70)
        
        # Initialize contributions tracking
        self.rollup_contributions = {}
        
        # Step 1: Get all leaf sites and process them first
        leaf_sites = self.get_all_leaf_sites(site_data)
        print(f"Found {len(leaf_sites)} leaf sites")
        
        processed_sites = set()
        
        # Step 2: Process each leaf site
        for leaf_site in leaf_sites:
            site_code = leaf_site['internal_site_code']
            print(f"\nProcessing leaf site: {site_code}")
            
            # Find cdata for this leaf site
            site_cdata = self.find_cdata_for_site(cdata_list, site_code, year, internal_code_id)
            
            for cdata in site_cdata:
                # Apply ownership to the cdata values
                original_qty = float(cdata.get('qty', 0) or 0)
                original_value = float(cdata.get('value', 0) or 0)
                ownership_factor = leaf_site.get('ownership', 100) / 100.0
                
                owned_qty = original_qty * ownership_factor
                owned_value = original_value * ownership_factor
                
                # Create new record for leaf site (no rollup_qty/rollup_value yet)
                rollup_record = self.create_rollup_record(cdata, leaf_site, 0, 0)
                self.new_rollup_table.append(rollup_record)
                
                print(f"  Added leaf record: qty={original_qty} -> owned_qty={owned_qty:.2f}, value={original_value} -> owned_value={owned_value:.2f}")
                
                # Track contribution to parent
                parent_site = self.get_parent_site(site_data, site_code)
                if parent_site:
                    parent_code = parent_site['internal_site_code']
                    if parent_code not in self.rollup_contributions:
                        self.rollup_contributions[parent_code] = []
                    
                    self.rollup_contributions[parent_code].append({
                        'child_site': site_code,
                        'code': cdata.get('code', ''),
                        'contrib_qty': owned_qty,
                        'contrib_value': owned_value,
                        'cdata': cdata
                    })
            
            processed_sites.add(site_code)
        
        # Step 3: Process parent sites level by level
        self.process_parent_sites(site_data, cdata_list, year, internal_code_id, processed_sites)
        
        print("\n" + "="*70)
        print(f"Rollup process completed! Created {len(self.new_rollup_table)} records in new table")
    
    def process_parent_sites(self, site_data: Dict, cdata_list: List[Dict], year: int, internal_code_id: str, processed_sites: set):
        """
        Process parent sites level by level, bottom-up
        """
        changed = True
        level = 1
        
        while changed:
            changed = False
            print(f"\n--- Processing Level {level} (Parent Sites) ---")
            
            sites_to_process = []
            self.collect_sites_to_process(site_data, processed_sites, sites_to_process)
            
            for site in sites_to_process:
                site_code = site['internal_site_code']
                print(f"\nProcessing parent site: {site_code}")
                
                # Check if this site has its own cdata
                site_cdata = self.find_cdata_for_site(cdata_list, site_code, year, internal_code_id)
                
                if site_cdata:
                    # This parent site has its own cdata
                    for cdata in site_cdata:
                        # Get rollup contributions from children for this specific code
                        rollup_qty = 0
                        rollup_value = 0
                        
                        if site_code in self.rollup_contributions:
                            for contrib in self.rollup_contributions[site_code]:
                                if contrib['code'] == cdata.get('code', ''):
                                    rollup_qty += contrib['contrib_qty']
                                    rollup_value += contrib['contrib_value']
                        
                        # Create rollup record with original data + rollup fields
                        rollup_record = self.create_rollup_record(cdata, site, rollup_qty, rollup_value)
                        self.new_rollup_table.append(rollup_record)
                        
                        print(f"  Added parent record: original qty={cdata.get('qty', 0)}, value={cdata.get('value', 0)}")
                        print(f"  Rollup contributions: qty={rollup_qty:.2f}, value={rollup_value:.2f}")
                        
                        # Calculate this site's contribution to its parent
                        original_qty = float(cdata.get('qty', 0) or 0)
                        original_value = float(cdata.get('value', 0) or 0)
                        ownership_factor = site.get('ownership', 100) / 100.0
                        
                        # Total contribution = (original + rollup) * ownership
                        total_qty = (original_qty + rollup_qty) * ownership_factor
                        total_value = (original_value + rollup_value) * ownership_factor
                        
                        # Track contribution to grandparent
                        parent_site = self.get_parent_site(site_data, site_code)
                        if parent_site:
                            parent_code = parent_site['internal_site_code']
                            if parent_code not in self.rollup_contributions:
                                self.rollup_contributions[parent_code] = []
                            
                            self.rollup_contributions[parent_code].append({
                                'child_site': site_code,
                                'code': cdata.get('code', ''),
                                'contrib_qty': total_qty,
                                'contrib_value': total_value,
                                'cdata': cdata
                            })
                else:
                    # This parent site has no cdata of its own, just pass through children's contributions
                    if site_code in self.rollup_contributions:
                        print(f"  Site {site_code} has no own cdata, passing through {len(self.rollup_contributions[site_code])} child contributions")
                        
                        # Group contributions by code
                        code_groups = {}
                        for contrib in self.rollup_contributions[site_code]:
                            code = contrib['code']
                            if code not in code_groups:
                                code_groups[code] = []
                            code_groups[code].append(contrib)
                        
                        # Pass through to grandparent
                        parent_site = self.get_parent_site(site_data, site_code)
                        if parent_site:
                            parent_code = parent_site['internal_site_code']
                            if parent_code not in self.rollup_contributions:
                                self.rollup_contributions[parent_code] = []
                            
                            for code, contribs in code_groups.items():
                                total_qty = sum(c['contrib_qty'] for c in contribs) * (site.get('ownership', 100) / 100.0)
                                total_value = sum(c['contrib_value'] for c in contribs) * (site.get('ownership', 100) / 100.0)
                                
                                self.rollup_contributions[parent_code].append({
                                    'child_site': site_code,
                                    'code': code,
                                    'contrib_qty': total_qty,
                                    'contrib_value': total_value,
                                    'cdata': contribs[0]['cdata']  # Use first cdata as template
                                })
                
                processed_sites.add(site_code)
                changed = True
            
            level += 1
            if level > 10:  # Safety break
                break
    
    def collect_sites_to_process(self, site_data: Dict, processed_sites: set, sites_to_process: List, current_site: Dict = None):
        """
        Collect sites that can be processed (all children are already processed)
        """
        if current_site is None:
            current_site = site_data
        
        site_code = current_site['internal_site_code']
        
        # Skip if already processed
        if site_code in processed_sites:
            if 'sites' in current_site:
                for child_site in current_site['sites']:
                    self.collect_sites_to_process(site_data, processed_sites, sites_to_process, child_site)
            return
        
        # Check if all children are processed
        all_children_processed = True
        if 'sites' in current_site and current_site['sites']:
            for child_site in current_site['sites']:
                if child_site['internal_site_code'] not in processed_sites:
                    all_children_processed = False
                    break
            
            # Continue with children
            for child_site in current_site['sites']:
                self.collect_sites_to_process(site_data, processed_sites, sites_to_process, child_site)
        
        # If all children are processed (or no children), this site can be processed
        if all_children_processed:
            sites_to_process.append(current_site)
    
    def get_rollup_table(self) -> List[Dict]:
        """
        Get the new rollup table
        """
        return self.new_rollup_table
    
    def print_rollup_table_summary(self):
        """
        Print summary of the rollup table
        """
        print("\n" + "="*70)
        print("ROLLUP TABLE SUMMARY")
        print("="*70)
        
        for i, record in enumerate(self.new_rollup_table, 1):
            print(f"\nRecord {i}:")
            print(f"  Site Code: {record.get('site_code', 'N/A')}")
            print(f"  Code: {record.get('code', 'N/A')}")
            print(f"  Original Qty: {record.get('qty', 0)}")
            print(f"  Original Value: {record.get('value', 0)}")
            print(f"  Rollup Qty: {record.get('rollup_qty', 0):.2f}")
            print(f"  Rollup Value: {record.get('rollup_value', 0):.2f}")
            print(f"  Site Ownership: {record.get('site_ownership', 100)}%")

# Example usage
def main():
    # Sample site data
    site_data = {
        "id": 437,
        "internal_site_code": "kiama",
        "site_type": "Head Office",
        "site_name": "Head Office",
        "company_id": "437",
        "ownership_status": "Owned",
        "ownership": 100,
        "value": 200,
        "sites": [
            {
                "id": 377,
                "internal_site_code": "HO_US",
                "site_type": "Head Office",
                "site_name": "Head Office US",
                "company_id": "437",
                "ownership_status": "Owned",
                "ownership": 10,
                "value": 1200,
                "sites": [
                    {
                        "id": 378,
                        "internal_site_code": "RO_US",
                        "site_name": "Regional Office US",
                        "company_id": "437",
                        "ownership_status": "Owned",
                        "ownership": 70,
                        "value": 50,
                        "sites": [
                            {
                                "id": 379,
                                "internal_site_code": "MS_US",
                                "site_type": "Manufacturing Site",
                                "zipcode": "53703",
                                "site_name": "Manufacturing Office US",
                                "company_id": "437",
                                "ownership_status": "Owned",
                                "ownership": 30,
                                "value": 20,
                                "sites": []
                            }
                        ]
                    }
                ]
            },
            {
                "id": 277,
                "internal_site_code": "HO_UK",
                "site_type": "Head Office",
                "site_name": "Head Office UK",
                "company_id": "437",
                "ownership_status": "Owned",
                "ownership": 80,
                "value": 500,
                "sites": [
                    {
                        "id": 278,
                        "internal_site_code": "RO_UK",
                        "site_name": "Regional Office UK",
                        "company_id": "437",
                        "ownership_status": "Owned",
                        "ownership": 40,
                        "value": 20,
                        "sites": [
                            {
                                "id": 279,
                                "internal_site_code": "MS_UK",
                                "site_type": "Manufacturing Site",
                                "zipcode": "53703",
                                "site_name": "Manufacturing Office UK",
                                "company_id": "437",
                                "ownership_status": "Owned",
                                "ownership": 60,
                                "value": 90,
                                "sites": []
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Sample cdata with multiple sites and same code
    sample_cdata = [
        # Leaf sites data
        {
            "company_code": "437",
            "type_year": 2023,
            "reporting_year": 2023,
            "qty": "100",
            "site_code": "MS_US",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code_name": "Anti-corruption policy",
            "code": "03-0030-0030-004",
            "value": 1000,
            "currency": "USD",
            "unit": "Y"
        },
        {
            "company_code": "437",
            "type_year": 2023,
            "reporting_year": 2023,
            "qty": "50",
            "site_code": "MS_UK",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code_name": "Anti-corruption policy",
            "code": "03-0030-0030-004",
            "value": 500,
            "currency": "GBP",
            "unit": "Y"
        },
        # Parent sites data
        {
            "company_code": "437",
            "type_year": 2023,
            "reporting_year": 2023,
            "qty": "25",
            "site_code": "RO_US",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code_name": "Anti-corruption policy",
            "code": "03-0030-0030-004",
            "value": 250,
            "currency": "USD",
            "unit": "Y"
        },
        {
            "company_code": "437",
            "type_year": 2023,
            "reporting_year": 2023,
            "qty": "75",
            "site_code": "kiama",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code_name": "Anti-corruption policy",
            "code": "03-0030-0030-004",
            "value": 750,
            "currency": "USD",
            "unit": "Y"
        }
    ]
    
    # Create rollup processor
    processor = SiteDataRollup()
    
    # Process the rollup for specific year and internal_code_id
    processor.process_site_rollup(
        site_data, 
        sample_cdata, 
        year=2023, 
        internal_code_id="660679405b653529f10e127b"
    )
    
    # Print summary
    processor.print_rollup_table_summary()
    
    # Get the new rollup table
    rollup_table = processor.get_rollup_table()
    
    # Save to file
    try:
        with open('new_rollup_table.json', 'w') as f:
            json.dump(rollup_table, f, indent=2, default=str)
        print(f"\nNew rollup table saved to 'new_rollup_table.json' with {len(rollup_table)} records")
    except Exception as e:
        print(f"Error saving file: {e}")

if __name__ == "__main__":
    main()