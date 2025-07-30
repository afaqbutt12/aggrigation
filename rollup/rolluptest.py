#!/usr/bin/env python3
"""
Test the rollup logic for all frequencies (monthly, quarterly, bi-annual, yearly) 
using real API and MongoDB data for company 482.
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
from rollcontroller import SiteDataRollup
from bson import ObjectId
from collections import defaultdict

def print_site_codes(site, level=0):
    print('  ' * level + f"site_code: {site.get('internal_site_code')}")
    for child in site.get('sites', []):
        print_site_codes(child, level+1)

def test_frequency_rollup(controller, company_id, frequency, collection_name, year=2023):
    """
    Test rollup for a specific frequency
    """
    print(f"\n{'='*80}")
    print(f"TESTING {frequency.upper()} ROLLUP")
    print(f"{'='*80}")
    
    # Get cdata for this frequency
    if frequency == 'monthly':
        cdata_cursor = controller.cdata_monthly.find({"company_code": company_id})
        collection = controller.cdata_monthly
    elif frequency == 'quarterly':
        cdata_cursor = controller.cdata_quarterly.find({"company_code": company_id})
        collection = controller.cdata_quarterly
    elif frequency == 'bi_annual':
        cdata_cursor = controller.cdata_bi_annual.find({"company_code": company_id})
        collection = controller.cdata_bi_annual
    else:  # yearly
        cdata_cursor = controller.cdata_yearly.find({"company_code": company_id})
        collection = controller.cdata_yearly
    
    cdata_list = list(cdata_cursor)
    print(f"Found {len(cdata_list)} {frequency} records for company {company_id}")
    
    if not cdata_list:
        print(f"No {frequency} data found for company {company_id}.")
        return
    
    # Group by (internal_code_id, year, period)
    if frequency == 'monthly':
        period_field = 'month'
    elif frequency == 'quarterly':
        period_field = 'quarter'
    elif frequency == 'bi_annual':
        period_field = 'semi_annual'
    else:
        period_field = None

    if period_field:
        code_year_period_map = defaultdict(list)
        for cdata in cdata_list:
            # Get internal_code_id as string
            internal_code_id = cdata.get("internal_code_id")
            if isinstance(internal_code_id, ObjectId):
                internal_code_id_str = str(internal_code_id)
            elif isinstance(internal_code_id, dict):
                internal_code_id_str = str(internal_code_id.get("$oid", ""))
            else:
                internal_code_id_str = str(internal_code_id)
            # Get year
            year_val = cdata.get("type_year") or cdata.get("reporting_year")
            if isinstance(year_val, str) and year_val.isdigit():
                year_val = int(year_val)
            period_val = cdata.get(period_field)
            code_year_period_map[(internal_code_id_str, year_val, period_val)].append(cdata)
        print(f"Found {len(code_year_period_map)} unique (internal_code_id, year, {period_field}) combinations for {frequency}")
        # Test all combinations
        for (internal_code_id, year_val, period_val), cdata_group in code_year_period_map.items():
            print(f"\n--- Testing {frequency} rollup for internal_code_id: {internal_code_id}, year: {year_val}, {period_field}: {period_val} ---")
            # Filter all cdata for this code/year/period
            relevant_cdata = [
                c for c in cdata_list
                if (
                    (
                        str(c.get('internal_code_id', '')) == internal_code_id or
                        (isinstance(c.get('internal_code_id'), ObjectId) and str(c.get('internal_code_id')) == internal_code_id) or
                        (isinstance(c.get('internal_code_id'), dict) and str(c.get('internal_code_id').get('$oid', '')) == internal_code_id)
                    )
                    and ((c.get('type_year') or c.get('reporting_year')) == year_val)
                    and (c.get(period_field) == period_val)
                )
            ]
            print(f"Relevant {frequency} cdata count for this group: {len(relevant_cdata)}")
            for rc in relevant_cdata:
                print(f"  - site_code: {rc.get('site_code')}, qty: {rc.get('qty', rc.get(' qty ', None))}, value: {rc.get('value')}, {period_field}: {rc.get(period_field)}")
            controller.processed_combinations.clear()
            controller.new_rollup_table = []
            # Run rollup with period
            result = controller.rollup_recursive(controller.site_data, relevant_cdata, year_val, internal_code_id, period_val=period_val, period_field=period_field)
            print(f"Root contribution: qty={result['own_contribution']['qty']}, value={result['own_contribution']['value']}")
            print(f"Total rollup from children: qty={result['total_rollup']['qty']}, value={result['total_rollup']['value']}")
            print(f"Rollup table records created: {len(controller.new_rollup_table)}")
            # Save to appropriate collection
            if frequency == 'monthly':
                controller.save_rollup_monthly_to_db()
            elif frequency == 'quarterly':
                controller.save_rollup_quarterly_to_db()
            elif frequency == 'bi_annual':
                controller.save_rollup_bi_annual_to_db()
            else:
                controller.save_rollup_yearly_to_db()
        print(f"\nCompleted {len(code_year_period_map)} {frequency} rollup tests")
    else:
        # Yearly fallback (original logic)
        code_year_map = defaultdict(list)
        for cdata in cdata_list:
            internal_code_id = cdata.get("internal_code_id")
            if isinstance(internal_code_id, ObjectId):
                internal_code_id_str = str(internal_code_id)
            elif isinstance(internal_code_id, dict):
                internal_code_id_str = str(internal_code_id.get("$oid", ""))
            else:
                internal_code_id_str = str(internal_code_id)
            year_val = cdata.get("type_year") or cdata.get("reporting_year")
            if isinstance(year_val, str) and year_val.isdigit():
                year_val = int(year_val)
            code_year_map[(internal_code_id_str, year_val)].append(cdata)
        print(f"Found {len(code_year_map)} unique (internal_code_id, year) pairs for {frequency}")
        for (internal_code_id, year_val), cdata_group in code_year_map.items():
            print(f"\n--- Testing {frequency} rollup for internal_code_id: {internal_code_id}, year: {year_val} ---")
            relevant_cdata = [
                c for c in cdata_list
                if (
                    (
                        str(c.get('internal_code_id', '')) == internal_code_id or
                        (isinstance(c.get('internal_code_id'), ObjectId) and str(c.get('internal_code_id')) == internal_code_id) or
                        (isinstance(c.get('internal_code_id'), dict) and str(c.get('internal_code_id').get('$oid', '')) == internal_code_id)
                    )
                    and ((c.get('type_year') or c.get('reporting_year')) == year_val)
                )
            ]
            print(f"Relevant {frequency} cdata count for this pair: {len(relevant_cdata)}")
            for rc in relevant_cdata:
                print(f"  - site_code: {rc.get('site_code')}, qty: {rc.get('qty', rc.get(' qty ', None))}, value: {rc.get('value')}")
            controller.processed_combinations.clear()
            controller.new_rollup_table = []
            result = controller.rollup_recursive(controller.site_data, relevant_cdata, year_val, internal_code_id)
            print(f"Root contribution: qty={result['own_contribution']['qty']}, value={result['own_contribution']['value']}")
            print(f"Total rollup from children: qty={result['total_rollup']['qty']}, value={result['total_rollup']['value']}")
            print(f"Rollup table records created: {len(controller.new_rollup_table)}")
            if frequency == 'monthly':
                controller.save_rollup_monthly_to_db()
            elif frequency == 'quarterly':
                controller.save_rollup_quarterly_to_db()
            elif frequency == 'bi_annual':
                controller.save_rollup_bi_annual_to_db()
            else:
                controller.save_rollup_yearly_to_db()
        print(f"\nCompleted {len(code_year_map)} {frequency} rollup tests")

def main():
    controller = SiteDataRollup()
    company_id = "482"
    
    print(f"Fetching site hierarchy for company {company_id}...")
    site_data = controller.fetch_site_data(company_id)
    if not site_data:
        print("Failed to fetch site hierarchy from API.")
        return
    print("Site hierarchy fetched.")
    
    # Store site data in controller for use in tests
    controller.site_data = site_data

    print("\nSite codes in hierarchy:")
    print_site_codes(site_data)

    # Test all frequencies
    frequencies = [
        ('monthly', 'cdata_monthly'),
        ('quarterly', 'cdata_quarterly'), 
        ('bi_annual', 'cdata_bi_annual'),
        ('yearly', 'cdata_yearly')
    ]
    
    for frequency, collection_name in frequencies:
        test_frequency_rollup(controller, company_id, frequency, collection_name)
    
    print(f"\n{'='*80}")
    print("ALL FREQUENCY TESTS COMPLETED")
    print(f"{'='*80}")
    
    # Print summary of collections
    print("\nCollection summaries:")
    for frequency, collection_name in frequencies:
        if frequency == 'monthly':
            count = controller.rollup_monthly.count_documents({})
        elif frequency == 'quarterly':
            count = controller.rollup_quarterly.count_documents({})
        elif frequency == 'bi_annual':
            count = controller.rollup_bi_annual.count_documents({})
        else:  # yearly
            count = controller.rollup_yearly.count_documents({})
        print(f"rollup_{frequency}: {count} documents")

if __name__ == "__main__":
    main() 