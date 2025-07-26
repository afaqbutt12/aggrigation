import json
import copy
from datetime import datetime
from typing import Dict, List, Any, Optional

class SiteDataRollup:
    def __init__(self):
        self.new_rollup_table = []
        self.processed_combinations = set()  # Track processed (site_code, year, internal_code_id)
    
    def find_latest_cdata_for_site(self, cdata_list: List[Dict], site_code: str, year: int, internal_code_id: str) -> Optional[Dict]:
        """
        Find the latest updated cdata record for a specific site, year, and internal_code_id combination
        Returns only ONE record - the most recently updated one
        """
        matching_records = []
        
        for cdata in cdata_list:
            # Match site_code, year, and internal_code_id
            cdata_year = cdata.get('type_year') or cdata.get('reporting_year')
            cdata_internal_id = str(cdata.get('internal_code_id', {}).get('$oid', ''))
            
            if (cdata.get('site_code') == site_code and 
                cdata_year == year and 
                cdata_internal_id == internal_code_id):
                matching_records.append(cdata)
        
        if not matching_records:
            return None
        
        # If only one record, return it
        if len(matching_records) == 1:
            return matching_records[0]
        
        # Multiple records found - pick the latest updated one
        latest_record = matching_records[0]
        latest_date = self.parse_date(latest_record.get('created_at') or latest_record.get('updated_at'))
        
        for record in matching_records[1:]:
            record_date = self.parse_date(record.get('created_at') or record.get('updated_at'))
            if record_date and (not latest_date or record_date > latest_date):
                latest_record = record
                latest_date = record_date
        
        print(f"    Found {len(matching_records)} records for {site_code}, using latest updated record")
        return latest_record
    
    def parse_date(self, date_field) -> Optional[datetime]:
        """
        Parse date from various formats
        """
        if not date_field:
            return None
        
        try:
            if isinstance(date_field, dict) and '$date' in date_field:
                date_str = date_field['$date']
            elif isinstance(date_field, str):
                date_str = date_field
            else:
                return None
            
            # Try to parse ISO format
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(date_str)
        except:
            return None
    
    def create_rollup_record(self, cdata: Dict, site: Dict, rollup_qty: float = 0, rollup_value: float = 0) -> Dict:
        """
        Create a new record for the rollup table
        """
        new_record = copy.deepcopy(cdata)
        
        # Add rollup fields
        new_record['rollup_qty'] = rollup_qty
        new_record['rollup_value'] = rollup_value
        new_record['site_ownership'] = site.get('ownership', 100)
        new_record['site_id'] = site['id']
        new_record['rollup_processed_at'] = {'$date': datetime.now().isoformat()}
        new_record['rollup_processed'] = True
        
        return new_record
    
    def rollup_recursive(self, site: Dict, cdata_list: List[Dict], year: int, internal_code_id: str, level: int = 0) -> Dict:
        """
        Efficient recursive function that processes sites and cdata in one pass
        Returns: {
            'own_contribution': {'qty': x, 'value': y},  # This site's contribution to parent
            'total_rollup': {'qty': x, 'value': y}       # Total rollup from all children
        }
        """
        site_cdata = self.find_latest_cdata_for_site(cdata_list, site_code, year, internal_code_id)
        indent = "  " * level
        site_code = site['internal_site_code']
        parent_qty= 0
        parent_value= 0
        if(site_cdata):   
            parent_qty= site_cdata['qty']
            parent_value= site_cdata['value']
        
        print(f"{indent}Processing site: {site_code} (Level {level})")
        
        # Initialize return values
        result = {
            'own_contribution': {'qty': 0, 'value': 0},
            'total_rollup': {'qty': parent_qty, 'value': parent_value}
        }
        
        # Step 1: Process all children first (post-order traversal)
        total_child_rollup_qty = 0
        total_child_rollup_value = 0
        
        if 'sites' in site and site['sites']:
            print(f"{indent}  Processing {len(site['sites'])} child sites...")
            for child_site in site['sites']:
                child_result = self.rollup_recursive(child_site, cdata_list, year, internal_code_id, level + 1)
                
                # Accumulate child contributions
                total_child_rollup_qty += child_result['own_contribution']['qty']
                total_child_rollup_value += child_result['own_contribution']['value']
                
                print(f"{indent}    Child {child_site['internal_site_code']} contributed: qty={child_result['own_contribution']['qty']:.2f}, value={child_result['own_contribution']['value']:.2f}")
        
        # Step 2: Check if this site has its own cdata
        combination_key = (site_code, year, internal_code_id)
        
        if combination_key not in self.processed_combinations:
            if site_cdata:
                print(f"{indent}  Found cdata for site {site_code}")
                
                # Get original values
                original_qty = float(site_cdata.get('qty', 0) or 0)
                original_value = float(site_cdata.get('value', 0) or 0)
                
                # Create rollup record with original data + rollup from children
                rollup_record = self.create_rollup_record(
                    site_cdata, 
                    site, 
                    total_child_rollup_qty, 
                    total_child_rollup_value
                )
                
                self.new_rollup_table.append(rollup_record)
                self.processed_combinations.add(combination_key)
                
                print(f"{indent}    Added record: original qty={original_qty}, value={original_value}")
                print(f"{indent}    Rollup from children: qty={total_child_rollup_qty:.2f}, value={total_child_rollup_value:.2f}")
                
                # Calculate this site's contribution to its parent
                # Contribution = (original + rollup from children) * ownership
                ownership_factor = site.get('ownership', 100) / 100.0
                
                total_qty_contribution = (original_qty + total_child_rollup_qty) * ownership_factor
                total_value_contribution = (original_value + total_child_rollup_value) * ownership_factor
                
                result['own_contribution'] = {
                    'qty': total_qty_contribution,
                    'value': total_value_contribution
                }
                
                print(f"{indent}    Contribution to parent: qty={total_qty_contribution:.2f}, value={total_value_contribution:.2f} (ownership: {site.get('ownership', 100)}%)")
                
            else:
                print(f"{indent}  No cdata found for site {site_code}")
                
                # No own cdata, just pass through children's contributions with ownership applied
                if total_child_rollup_qty > 0 or total_child_rollup_value > 0:
                    ownership_factor = site.get('ownership', 100) / 100.0
                    
                    result['own_contribution'] = {
                        'qty': total_child_rollup_qty * ownership_factor,
                        'value': total_child_rollup_value * ownership_factor
                    }
                    
                    print(f"{indent}    Passing through children's contributions with ownership: qty={result['own_contribution']['qty']:.2f}, value={result['own_contribution']['value']:.2f}")
        else:
            print(f"{indent}  Site {site_code} already processed for this combination")
        
        # Store total rollup for reference
        result['total_rollup'] = {
            'qty': total_child_rollup_qty,
            'value': total_child_rollup_value
        }
        
        return result
    
    def process_rollup(self, site_data: Dict, cdata_list: List[Dict], year: int, internal_code_id: str):
        """
        Main entry point for rollup processing
        """
        print(f"Starting efficient recursive rollup for year {year}, internal_code_id {internal_code_id}")
        print("="*80)
        
        # Reset state
        self.new_rollup_table = []
        self.processed_combinations = set()
        
        # Start recursive processing from root
        root_result = self.rollup_recursive(site_data, cdata_list, year, internal_code_id)
        
        print("\n" + "="*80)
        print(f"Rollup completed! Created {len(self.new_rollup_table)} records")
        print(f"Root site total contribution: qty={root_result['own_contribution']['qty']:.2f}, value={root_result['own_contribution']['value']:.2f}")
    
    def get_rollup_table(self) -> List[Dict]:
        """
        Get the new rollup table
        """
        return self.new_rollup_table
    
    def print_rollup_table_summary(self):
        """
        Print detailed summary of the rollup table
        """
        print("\n" + "="*80)
        print("NEW ROLLUP TABLE SUMMARY")
        print("="*80)
        
        for i, record in enumerate(self.new_rollup_table, 1):
            print(f"\nRecord {i}:")
            print(f"  Site Code: {record.get('site_code', 'N/A')}")
            print(f"  Code: {record.get('code', 'N/A')} - {record.get('code_name', 'N/A')}")
            print(f"  Year: {record.get('type_year', record.get('reporting_year', 'N/A'))}")
            print(f"  Original Qty: {record.get('qty', 0)}")
            print(f"  Original Value: {record.get('value', 0)}")
            print(f"  Rollup Qty (from children): {record.get('rollup_qty', 0):.2f}")
            print(f"  Rollup Value (from children): {record.get('rollup_value', 0):.2f}")
            print(f"  Site Ownership: {record.get('site_ownership', 100)}%")
            print(f"  Total Effective Qty: {float(record.get('qty', 0) or 0) + record.get('rollup_qty', 0):.2f}")
            print(f"  Total Effective Value: {float(record.get('value', 0) or 0) + record.get('rollup_value', 0):.2f}")
    
    def export_to_database_format(self) -> List[Dict]:
        """
        Export rollup table in format ready for database insertion
        """
        return [
            {
                'site_code': record.get('site_code'),
                'site_id': record.get('site_id'),
                'company_code': record.get('company_code'),
                'year': record.get('type_year', record.get('reporting_year')),
                'internal_code_id': record.get('internal_code_id'),
                'code': record.get('code'),
                'code_name': record.get('code_name'),
                'original_qty': record.get('qty'),
                'original_value': record.get('value'),
                'rollup_qty': record.get('rollup_qty'),
                'rollup_value': record.get('rollup_value'),
                'total_qty': float(record.get('qty', 0) or 0) + record.get('rollup_qty', 0),
                'total_value': float(record.get('value', 0) or 0) + record.get('rollup_value', 0),
                'ownership_percentage': record.get('site_ownership'),
                'currency': record.get('currency'),
                'unit': record.get('unit'),
                'processed_at': record.get('rollup_processed_at'),
                'is_rollup_record': True
            }
            for record in self.new_rollup_table
        ]

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
    
    # Sample cdata with multiple records for same combination (to test latest selection)
    sample_cdata = [
        # Multiple records for MS_US - should pick the latest
        {
            "company_code": "437",
            "type_year": 2023,
            "qty": "100",
            "site_code": "MS_US",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code": "03-0030-0030-004",
            "code_name": "Anti-corruption policy",
            "value": 1000,
            "currency": "USD",
            "unit": "Y",
            "created_at": {"$date": "2023-01-01T10:00:00.000Z"}
        },
        {
            "company_code": "437",
            "type_year": 2023,
            "qty": "120",  # This should be picked (later date)
            "site_code": "MS_US",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code": "03-0030-0030-004",
            "code_name": "Anti-corruption policy",
            "value": 1200,
            "currency": "USD",
            "unit": "Y",
            "created_at": {"$date": "2023-01-02T10:00:00.000Z"}  # Later date
        },
        # Other sites
        {
            "company_code": "437",
            "type_year": 2023,
            "qty": "50",
            "site_code": "MS_UK",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code": "03-0030-0030-004",
            "code_name": "Anti-corruption policy",
            "value": 500,
            "currency": "GBP",
            "unit": "Y",
            "created_at": {"$date": "2023-01-01T10:00:00.000Z"}
        },
        {
            "company_code": "437",
            "type_year": 2023,
            "qty": "25",
            "site_code": "RO_US",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code": "03-0030-0030-004",
            "code_name": "Anti-corruption policy",
            "value": 250,
            "currency": "USD",
            "unit": "Y",
            "created_at": {"$date": "2023-01-01T10:00:00.000Z"}
        },
        {
            "company_code": "437",
            "type_year": 2023,
            "qty": "75",
            "site_code": "kiama",
            "internal_code_id": {"$oid": "660679405b653529f10e127b"},
            "code": "03-0030-0030-004",
            "code_name": "Anti-corruption policy",
            "value": 750,
            "currency": "USD",
            "unit": "Y",
            "created_at": {"$date": "2023-01-01T10:00:00.000Z"}
        }
    ]
    
    # Create rollup processor
    processor = SiteDataRollup()
    
    # Process the rollup
    processor.process_rollup(
        site_data, 
        sample_cdata, 
        year=2023, 
        internal_code_id="660679405b653529f10e127b"
    )
    
    # Print detailed summary
    processor.print_rollup_table_summary()
    
    # Get database-ready format
    db_records = processor.export_to_database_format()
    
    # Save results
    try:
        with open('new_rollup_table.json', 'w') as f:
            json.dump(processor.get_rollup_table(), f, indent=2, default=str)
        
        with open('db_ready_rollup.json', 'w') as f:
            json.dump(db_records, f, indent=2, default=str)
        
        print(f"\nFiles saved:")
        print(f"- new_rollup_table.json ({len(processor.get_rollup_table())} records)")
        print(f"- db_ready_rollup.json ({len(db_records)} records)")
    except Exception as e:
        print(f"Error saving files: {e}")

if __name__ == "__main__":
    main()