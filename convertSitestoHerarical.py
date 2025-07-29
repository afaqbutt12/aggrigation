def convert_flat_sites_to_hierarchy(flat_sites_array):
    """
    Convert flat array of sites into hierarchical structure based on parentSiteCode relationships
    
    Args:
        flat_sites_array: List of site dictionaries from API
        
    Returns:
        Dict containing hierarchical structure with root_sites, all_sites, and site_dict
    """
    
    if not flat_sites_array or not isinstance(flat_sites_array, list):
        return {
            'root_sites': [],
            'all_sites': [],
            'site_dict': {}
        }
    
    # Create a deep copy to avoid modifying original data
    import copy
    sites = copy.deepcopy(flat_sites_array)
    
    # Create a dictionary for quick lookup by internal_site_code
    site_dict = {site['internal_site_code']: site for site in sites}
    
    # Initialize each site with empty children array and fix ownership
    for site in sites:
        site['sites'] = []  # This will hold child sites
        
        # Handle ownership field - convert to float and handle empty/None values
        ownership = site.get('ownership', 100)
        if ownership == '' or ownership is None:
            site['ownership'] = 100.0  # Default to 100% if empty
        else:
            try:
                site['ownership'] = float(ownership)
            except (ValueError, TypeError):
                site['ownership'] = 100.0
        
        # Ensure company_industries is properly named (it's called company_industries in your data)
        if 'company_industries' in site:
            site['site_industries'] = site['company_industries']
    
    # Build parent-child relationships
    root_sites = []
    
    for site in sites:
        parent_code = site.get('parentSiteCode', '').strip()
        
        if not parent_code:  # This is a root site
            root_sites.append(site)
            print(f"Found root site: {site['internal_site_code']}")
        else:
            # This is a child site, find its parent
            if parent_code in site_dict:
                parent_site = site_dict[parent_code]
                parent_site['sites'].append(site)
                print(f"Added {site['internal_site_code']} as child of {parent_code}")
            else:
                # Parent not found in current dataset, treat as root
                print(f"Warning: Parent site {parent_code} not found for {site['internal_site_code']}, treating as root")
                root_sites.append(site)
    
    print(f"Built hierarchy with {len(root_sites)} root sites and {len(sites)} total sites")
    
    return {
        'root_sites': root_sites,
        'all_sites': sites,
        'site_dict': site_dict
    }

def print_hierarchy_tree(hierarchy, level=0, max_levels=None):
    """
    Print the site hierarchy in a tree format
    """
    if not hierarchy.get('root_sites'):
        print("No root sites found")
        return
    
    for root_site in hierarchy['root_sites']:
        _print_site_recursive(root_site, level, max_levels)

def _print_site_recursive(site, level=0, max_levels=None):
    """
    Recursively print site hierarchy with tree structure
    """
    if max_levels is not None and level >= max_levels:
        return
        
    indent = "  " * level
    site_code = site.get('internal_site_code', 'unknown')
    site_name = site.get('site_name', 'Unknown')
    ownership = site.get('ownership', 100)
    child_count = len(site.get('sites', []))
    
    # Use tree characters
    if level == 0:
        prefix = ""
    else:
        prefix = "├── " if level > 0 else ""
    
    print(f"{indent}{prefix}{site_code} ({site_name}) - {ownership}% ownership, {child_count} children")
    
    for i, child_site in enumerate(site.get('sites', [])):
        _print_site_recursive(child_site, level + 1, max_levels)

def get_hierarchy_stats(hierarchy):
    """
    Get statistics about the hierarchy
    """
    if not hierarchy or not hierarchy.get('all_sites'):
        return {}
    
    sites = hierarchy['all_sites']
    root_sites = hierarchy.get('root_sites', [])
    
    # Count sites by level
    level_counts = {}
    
    def count_levels(site, level=0):
        if level not in level_counts:
            level_counts[level] = 0
        level_counts[level] += 1
        
        for child in site.get('sites', []):
            count_levels(child, level + 1)
    
    for root_site in root_sites:
        count_levels(root_site)
    
    # Count by parent
    parent_counts = {}
    ownership_by_parent = {}
    
    for site in sites:
        parent_code = site.get('parentSiteCode', 'ROOT')
        if not parent_code:
            parent_code = 'ROOT'
        
        if parent_code not in parent_counts:
            parent_counts[parent_code] = 0
            ownership_by_parent[parent_code] = []
        
        parent_counts[parent_code] += 1
        ownership_by_parent[parent_code].append(site.get('ownership', 100))
    
    # Industry distribution
    industries = {}
    for site in sites:
        site_industries = site.get('site_industries', [])
        for industry_info in site_industries:
            industry = industry_info.get('industry', 'Unknown')
            if industry not in industries:
                industries[industry] = 0
            industries[industry] += 1
    
    return {
        'total_sites': len(sites),
        'root_sites_count': len(root_sites),
        'max_depth': max(level_counts.keys()) if level_counts else 0,
        'level_counts': level_counts,
        'parent_counts': parent_counts,
        'ownership_by_parent': ownership_by_parent,
        'industries': industries
    }



def integrate_with_rollup_script(flat_sites_array):
    """
    Function specifically designed to integrate with your rollup script
    Returns the exact format expected by process_site_hierarchy method
    """
    return convert_flat_sites_to_hierarchy(flat_sites_array)
