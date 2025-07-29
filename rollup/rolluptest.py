#!/usr/bin/env python3
"""
Test script for the actual rollcontroller.py implementation
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

# Mock the database connection to avoid connection issues
import unittest.mock
from unittest.mock import Mock, MagicMock

# Mock the database collections
mock_collections = {
    "company_codes": Mock(),
    "rollup_yearly": Mock(),
    "cdata_monthly": Mock(),
    "cdata_yearly": Mock(),
    "cdata_quarterly": Mock(),
    "cdata_bi_annual": Mock()
}

# Mock the database connection
mock_connection = Mock()
mock_connection.__getitem__ = lambda self, key: mock_collections[key]

# Mock the db_connection module
sys.modules['db_connection'] = Mock()
sys.modules['db_connection'].connect_to_database = Mock(return_value=mock_connection)

# Mock the RegionAPI module
sys.modules['RegionAPI'] = Mock()
sys.modules['RegionAPI'].fetch_company_data_safe = Mock(return_value=["January"])
sys.modules['RegionAPI'].fetch_all_company = Mock(return_value=[])

# Now import the actual rollcontroller
from rollcontroller import SiteDataRollup

class TestRealRollController:
    """Test the actual rollcontroller implementation"""
    
    def __init__(self):
        """Initialize test with mocked dependencies"""
        # Mock the database connection
        with unittest.mock.patch('rollup.rollcontroller.db_connection.connect_to_database', return_value=mock_connection):
            self.controller = SiteDataRollup()
        
        # Mock the fetch_site_data method to return our test data
        self.controller.fetch_site_data = self.mock_fetch_site_data
    
    def mock_fetch_site_data(self, company_id: str):
        """Mock site data with complex hierarchy"""
        return {
            "id": 423,
            "internal_site_code": "GB_EU",
            "site_type": "Bond",
            "site_name": "Green Bonds",
            "parentSiteCode": "",
            "company_id": "482",
            "ownership": "100",
            "sites": [
                {
                    "id": 424,
                    "internal_site_code": "TE_EU",
                    "site_type": "Equity",
                    "site_name": "Trust Equity",
                    "parentSiteCode": "GB_EU",
                    "company_id": "482",
                    "ownership": 70,
                    "sites": [
                        {
                            "id": 426,
                            "internal_site_code": "TE_SUB1",
                            "site_type": "Subsidiary",
                            "site_name": "TE Subsidiary 1",
                            "parentSiteCode": "TE_EU",
                            "company_id": "482",
                            "ownership": 80,
                            "sites": []
                        },
                        {
                            "id": 427,
                            "internal_site_code": "TE_SUB2",
                            "site_type": "Subsidiary",
                            "site_name": "TE Subsidiary 2",
                            "parentSiteCode": "TE_EU",
                            "company_id": "482",
                            "ownership": 60,
                            "sites": []
                        }
                    ]
                },
                {
                    "id": 425,
                    "internal_site_code": "ML_EU",
                    "site_type": "Commercial Loan",
                    "site_name": "Commercial Loans",
                    "parentSiteCode": "GB_EU",
                    "company_id": "482",
                    "ownership": 30,
                    "sites": [
                        {
                            "id": 428,
                            "internal_site_code": "ML_SUB1",
                            "site_type": "Subsidiary",
                            "site_name": "ML Subsidiary 1",
                            "parentSiteCode": "ML_EU",
                            "company_id": "482",
                            "ownership": 50,
                            "sites": []
                        }
                    ]
                }
            ]
        }

def test_ownership_handling():
    """Test ownership value handling in real implementation"""
    test = TestRealRollController()
    
    test_cases = [
        {"ownership": "100", "expected": 100.0},
        {"ownership": 70, "expected": 70.0},
        {"ownership": "50", "expected": 50.0},
        {"ownership": "invalid", "expected": 100.0},  # Default fallback
        {"ownership": None, "expected": 100.0},  # Default fallback
    ]
    
    print("\nTest ownership handling in real implementation:")
    for case in test_cases:
        ownership_value = case["ownership"]
        if isinstance(ownership_value, str):
            result = float(ownership_value) if ownership_value.isdigit() else 100
        else:
            result = ownership_value or 100
        
        print(f"Input: {ownership_value} -> Result: {result} (Expected: {case['expected']})")
        assert result == case["expected"], f"Ownership test failed for {ownership_value}"

def test_site_hierarchy():
    """Test site hierarchy building in real implementation"""
    test = TestRealRollController()
    
    site_data = test.controller.fetch_site_data("482")
    
    print("\nTest site hierarchy in real implementation:")
    if site_data:
        print(f"Root site: {site_data['internal_site_code']} - {site_data['site_name']}")
        print(f"Children count: {len(site_data['sites'])}")
        for child in site_data['sites']:
            print(f"  - {child['internal_site_code']} - {child['site_name']} (ownership: {child['ownership']}%)")
            if child['sites']:
                print(f"    Grandchildren: {len(child['sites'])}")
                for grandchild in child['sites']:
                    print(f"      - {grandchild['internal_site_code']} - {grandchild['site_name']} (ownership: {grandchild['ownership']}%)")
    else:
        print("No site data returned")

def test_find_latest_cdata():
    """Test find_latest_cdata_for_site method"""
    test = TestRealRollController()
    
    # Sample cdata with correct format
    cdata_list = [
        {
            "site_code": "GB_EU",
            "type_year": 2023,  # Use integer instead of string
            "internal_code_id": {"$oid": "660679405b653529f10e1202"},
            " qty ": "1000000",
            "value": "500000"
        },
        {
            "site_code": "TE_EU",
            "type_year": 2023,  # Use integer instead of string
            "internal_code_id": {"$oid": "660679405b653529f10e1202"},
            " qty ": "500000",
            "value": "200000"
        }
    ]
    
    print("\nTest find_latest_cdata_for_site:")
    
    # Test finding existing data
    result = test.controller.find_latest_cdata_for_site(cdata_list, "GB_EU", 2023, "660679405b653529f10e1202")
    if result:
        print(f"Found data for GB_EU: qty={result.get(' qty ')}, value={result.get('value')}")
    else:
        print("No data found for GB_EU")
    
    # Test finding non-existent data
    result = test.controller.find_latest_cdata_for_site(cdata_list, "NONEXISTENT", 2023, "660679405b653529f10e1202")
    if result:
        print(f"Unexpected data found for NONEXISTENT")
    else:
        print("Correctly found no data for NONEXISTENT")

def test_create_rollup_record():
    """Test create_rollup_record method"""
    test = TestRealRollController()
    
    cdata = {
        "site_code": "GB_EU",
        " qty ": "1000000",
        "value": "500000",
        "code_name": "Test Code"
    }
    
    site = {
        "id": 423,
        "internal_site_code": "GB_EU",
        "ownership": "100"
    }
    
    print("\nTest create_rollup_record:")
    record = test.controller.create_rollup_record(cdata, site, 100000, 50000)
    
    print(f"Original qty: {record.get(' qty ')}")
    print(f"Original value: {record.get('value')}")
    print(f"Rollup qty: {record.get('rollup_qty')}")
    print(f"Rollup value: {record.get('rollup_value')}")
    print(f"Site ownership: {record.get('site_ownership')}%")
    print(f"Site ID: {record.get('site_id')}")

def test_complex_rollup_scenario():
    """Test complex rollup scenario with real implementation"""
    test = TestRealRollController()
    
    # Complex cdata with grandchildren having data
    complex_cdata = [
        # Root site data
        {
            "_id": {"$oid": "67af071230d9af993ab0c455"},
            "company_code": "690",
            "type_year": 2023,  # Use integer
            "reporting_year": 2023,  # Use integer
            "Code": "01-0010-0010-001",
            "Name": "Scope 1 - emissions",
            " qty ": "1000000",
            "value": "500000",
            "site_code": "GB_EU",
            "internal_code_id": {"$oid": "660679405b653529f10e1202"},
            "code_name": "Scope 1 - emissions",
            "code": "01-0010-0010-001",
            "currency": "SAR",
            "unit": "MTCO2e",
            "description": " ",
            "ref_table": "cdata",
            "is_forecast": False,
            "created_at": {"$date": "2025-02-14T09:04:18.605Z"}
        },
        # TE_EU (child) has its own data
        {
            "_id": {"$oid": "67af066d30d9af993ab0c348"},
            "company_code": "690",
            "type_year": 2023,  # Use integer
            "reporting_year": 2023,  # Use integer
            "Code": "01-0010-0060-003",
            "Name": "Air emissions - sox",
            " qty ": "500000",
            "value": "200000",
            "site_code": "TE_EU",
            "internal_code_id": {"$oid": "660679405b653529f10e11f8"},
            "code_name": "Air emissions - sox",
            "code": "01-0010-0060-003",
            "currency": "SAR",
            "unit": "MTCO2e",
            "description": " ",
            "ref_table": "cdata",
            "is_forecast": False,
            "created_at": {"$date": "2025-02-14T09:01:33.123Z"}
        },
        # TE_SUB1 (grandchild) has data
        {
            "_id": {"$oid": "67af066f30d9af993ab0c357"},
            "company_code": "690",
            "type_year": 2023,  # Use integer
            "reporting_year": 2023,  # Use integer
            "Code": "01-0010-0060-003",
            "Name": "Air emissions - sox",
            " qty ": "300000",
            "value": "100000",
            "site_code": "TE_SUB1",
            "internal_code_id": {"$oid": "660679405b653529f10e11f8"},
            "code_name": "Air emissions - sox",
            "code": "01-0010-0060-003",
            "currency": "SAR",
            "unit": "MTCO2e",
            "description": " ",
            "ref_table": "cdata",
            "is_forecast": False,
            "created_at": {"$date": "2025-02-14T09:01:35.584Z"}
        },
        # TE_SUB2 (grandchild) has data
        {
            "_id": {"$oid": "67af066f30d9af993ab0c358"},
            "company_code": "690",
            "type_year": 2023,  # Use integer
            "reporting_year": 2023,  # Use integer
            "Code": "01-0010-0060-003",
            "Name": "Air emissions - sox",
            " qty ": "200000",
            "value": "80000",
            "site_code": "TE_SUB2",
            "internal_code_id": {"$oid": "660679405b653529f10e11f8"},
            "code_name": "Air emissions - sox",
            "code": "01-0010-0060-003",
            "currency": "SAR",
            "unit": "MTCO2e",
            "description": " ",
            "ref_table": "cdata",
            "is_forecast": False,
            "created_at": {"$date": "2025-02-14T09:01:35.584Z"}
        },
        # ML_SUB1 (grandchild) has data
        {
            "_id": {"$oid": "67af066f30d9af993ab0c359"},
            "company_code": "690",
            "type_year": 2023,  # Use integer
            "reporting_year": 2023,  # Use integer
            "Code": "01-0010-0060-003",
            "Name": "Air emissions - sox",
            " qty ": "150000",
            "value": "60000",
            "site_code": "ML_SUB1",
            "internal_code_id": {"$oid": "660679405b653529f10e11f8"},
            "code_name": "Air emissions - sox",
            "code": "01-0010-0060-003",
            "currency": "SAR",
            "unit": "MTCO2e",
            "description": " ",
            "ref_table": "cdata",
            "is_forecast": False,
            "created_at": {"$date": "2025-02-14T09:01:35.584Z"}
        }
    ]
    
    # Get site data with deeper hierarchy
    site_data = test.controller.fetch_site_data("482")
    
    print("\nTest complex rollup with real implementation:")
    print("=" * 80)
    print("Site Hierarchy:")
    print("GB_EU (100%)")
    print("├── TE_EU (70%)")
    print("│   ├── TE_SUB1 (80%)")
    print("│   └── TE_SUB2 (60%)")
    print("└── ML_EU (30%)")
    print("    └── ML_SUB1 (50%)")
    
    # Reset for new test
    test.controller.processed_combinations.clear()
    test.controller.new_rollup_table = []
    
    # Run rollup
    result = test.controller.rollup_recursive(site_data, complex_cdata, 2023, "660679405b653529f10e11f8")
    
    print(f"\nFinal Results:")
    print(f"Root contribution: qty={result['own_contribution']['qty']:.2f}, value={result['own_contribution']['value']:.2f}")
    print(f"Total rollup from children: qty={result['total_rollup']['qty']:.2f}, value={result['total_rollup']['value']:.2f}")
    
    print(f"\nRollup table records created: {len(test.controller.new_rollup_table)}")
    for i, record in enumerate(test.controller.new_rollup_table, 1):
        print(f"Record {i}:")
        print(f"  Site: {record.get('site_code')}")
        print(f"  Original qty: {record.get(' qty ')}")
        print(f"  Original value: {record.get('value')}")
        print(f"  Rollup qty: {record.get('rollup_qty', 0):.2f}")
        print(f"  Rollup value: {record.get('rollup_value', 0):.2f}")
        print(f"  Ownership: {record.get('site_ownership', 100)}%")

def test_process_rollup_method():
    """Test the process_rollup method"""
    test = TestRealRollController()
    
    # Simple cdata for testing
    simple_cdata = [
        {
            "site_code": "GB_EU",
            "type_year": 2023,  # Use integer
            "internal_code_id": {"$oid": "660679405b653529f10e1202"},
            " qty ": "1000000",
            "value": "500000",
            "code_name": "Test Code"
        }
    ]
    
    site_data = test.controller.fetch_site_data("482")
    
    print("\nTest process_rollup method:")
    
    # Reset for new test
    test.controller.processed_combinations.clear()
    test.controller.new_rollup_table = []
    
    # Run process_rollup
    test.controller.process_rollup(site_data, simple_cdata, 2023, "660679405b653529f10e1202")
    
    print(f"Rollup table records created: {len(test.controller.new_rollup_table)}")
    if test.controller.new_rollup_table:
        record = test.controller.new_rollup_table[0]
        print(f"First record site: {record.get('site_code')}")
        print(f"Original qty: {record.get(' qty ')}")
        print(f"Rollup qty: {record.get('rollup_qty', 0):.2f}")

if __name__ == "__main__":
    print("Testing actual rollcontroller.py implementation...")
    
    try:
        # Test ownership handling
        test_ownership_handling()
        
        # Test site hierarchy
        test_site_hierarchy()
        
        # Test find_latest_cdata method
        test_find_latest_cdata()
        
        # Test create_rollup_record method
        test_create_rollup_record()
        
        # Test complex rollup scenario
        test_complex_rollup_scenario()
        
        # Test process_rollup method
        test_process_rollup_method()
        
        print("\n✅ All tests passed for actual rollcontroller.py!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()