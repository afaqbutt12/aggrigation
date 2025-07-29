#!/usr/bin/env python3
"""
Test the real rollup logic on all available internal_code_id/type_year pairs for company 482 using real API and MongoDB data.
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

def main():
    controller = SiteDataRollup()
    company_id = "482"
    print(f"Fetching site hierarchy for company {company_id}...")
    site_data = controller.fetch_site_data(company_id)
    if not site_data:
        print("Failed to fetch site hierarchy from API.")
        return
    print("Site hierarchy fetched.")

    print("\nSite codes in hierarchy:")
    print_site_codes(site_data)

    print("Fetching all cdata_yearly for company 482...")
    cdata_cursor = controller.cdata_yearly.find({"company_code": company_id})
    cdata_list = list(cdata_cursor)
    print(f"Found {len(cdata_list)} cdata_yearly records.")
    if not cdata_list:
        print("No cdata_yearly data found for company 482.")
        return

    # Target record info
    TARGET_INTERNAL_CODE_ID = '660679415b653529f10e1309'
    TARGET_YEAR = 2027
    found_in_db = False
    found_in_grouped = False
    found_in_rollup = False

    # Debug: Print all internal_code_id/year pairs and types
    print("\nAll (internal_code_id, year) pairs in DB:")
    for c in cdata_list:
        icid = c.get('internal_code_id')
        if isinstance(icid, ObjectId):
            icid_str = str(icid)
        elif isinstance(icid, dict):
            icid_str = str(icid.get('$oid', ''))
        else:
            icid_str = str(icid)
        year = c.get('type_year') or c.get('reporting_year')
        print(f"internal_code_id: {icid_str} (type: {type(icid)}), year: {year} (type: {type(year)}), code: {c.get('code')}, site_code: {c.get('site_code')}")
        if icid_str == TARGET_INTERNAL_CODE_ID and int(year) == TARGET_YEAR:
            print("*** THIS IS YOUR NEW TARGET RECORD ***")
            found_in_db = True

    # Group by (internal_code_id, type_year)
    code_year_map = defaultdict(list)
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
        year = cdata.get("type_year") or cdata.get("reporting_year")
        if isinstance(year, str) and year.isdigit():
            year = int(year)
        code_year_map[(internal_code_id_str, year)].append(cdata)

    print(f"\nFound {len(code_year_map)} unique (internal_code_id, year) pairs.")
    print("All grouped pairs:")
    for k in code_year_map:
        print(k)
        if k == (TARGET_INTERNAL_CODE_ID, TARGET_YEAR):
            print("*** TARGET PAIR FOUND IN GROUPED PAIRS ***")
            found_in_grouped = True

    # For each (internal_code_id, year), run rollup
    for (internal_code_id, year), cdata_group in code_year_map.items():
        is_target = (internal_code_id == TARGET_INTERNAL_CODE_ID and year == TARGET_YEAR)
        print("\n" + "="*80)
        print(f"Running rollup for internal_code_id: {internal_code_id}, year: {year}")
        # Filter all cdata for this code/year
        relevant_cdata = [
            c for c in cdata_list
            if (
                (
                    str(c.get('internal_code_id', '')) == internal_code_id or
                    (isinstance(c.get('internal_code_id'), ObjectId) and str(c.get('internal_code_id')) == internal_code_id) or
                    (isinstance(c.get('internal_code_id'), dict) and str(c.get('internal_code_id').get('$oid', '')) == internal_code_id)
                )
                and
                ((c.get('type_year') or c.get('reporting_year')) == year)
            )
        ]
        print(f"Relevant cdata count for this pair: {len(relevant_cdata)}")
        for rc in relevant_cdata:
            print(f"  - site_code: {rc.get('site_code')}, qty: {rc.get('qty', rc.get(' qty ', None))}, value: {rc.get('value')}")
        controller.processed_combinations.clear()
        controller.new_rollup_table = []
        result = controller.rollup_recursive(site_data, relevant_cdata, year, internal_code_id)
        controller.save_rollup_to_db()
        print(f"Root contribution: qty={result['own_contribution']['qty']}, value={result['own_contribution']['value']}")
        print(f"Total rollup from children: qty={result['total_rollup']['qty']}, value={result['total_rollup']['value']}")
        print(f"Rollup table records created: {len(controller.new_rollup_table)}")
        if controller.new_rollup_table:
            print(f"First record: {controller.new_rollup_table[0]}")
        if is_target:
            found_in_rollup = True
            print("*** TARGET PAIR ROLLUP EXECUTED ***")

    print("\n================ SUMMARY ================")
    print(f"Target record (internal_code_id: {TARGET_INTERNAL_CODE_ID}, year: {TARGET_YEAR}) found in DB: {found_in_db}")
    print(f"Target pair found in grouped pairs: {found_in_grouped}")
    print(f"Target rollup executed: {found_in_rollup}")
    if not found_in_db:
        print("Target record is NOT present in the DB query result. Check your DB and query filter.")
    elif not found_in_grouped:
        print("Target record is present in DB but NOT grouped for rollup. Check grouping logic.")
    elif not found_in_rollup:
        print("Target record is grouped but rollup was NOT executed. Check rollup loop.")
    else:
        print("Target record was processed end-to-end. If you still have issues, check the rollup logic itself.")

if __name__ == "__main__":
    main() 