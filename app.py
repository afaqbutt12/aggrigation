import json
from bson import ObjectId
from flask import Flask, jsonify, request
from flask_cors import CORS
import logging
import sys
import os
import threading
import time
from dotenv import load_dotenv
import traceback
import main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), './rollup')))
import rollcontroller

# Load environment variables
load_dotenv()
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

# Global dictionary to track running threads
running_threads = {}
thread_lock = threading.Lock()

def convert_objectids_to_strings(data):
    """Recursively convert ObjectId instances to strings"""
    if isinstance(data, dict):
        return {key: convert_objectids_to_strings(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_objectids_to_strings(item) for item in data]
    elif isinstance(data, ObjectId):
        return str(data)
    else:
        return data

def run_aggregation_script_in_background(company_id, thread_id):
    """Run the aggregation script in a background thread"""
    try:
        with thread_lock:
            running_threads[thread_id] = {
                'status': 'running',
                'company_id': company_id,
                'start_time': time.time(),
                'type': 'aggregation'
            }
        
        logger.info(f"Starting aggregation script for company_id: {company_id}")
        result = main.main(company_id=company_id)
        
        # Update thread status
        with thread_lock:
            if thread_id in running_threads:
                running_threads[thread_id]['status'] = 'completed'
                running_threads[thread_id]['result'] = result
                running_threads[thread_id]['end_time'] = time.time()
        
        logger.info("Aggregation script completed successfully")
        
    except Exception as e:
        logger.error(f"Error in background aggregation: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update thread status with error
        with thread_lock:
            if thread_id in running_threads:
                running_threads[thread_id]['status'] = 'error'
                running_threads[thread_id]['error'] = str(e)
                running_threads[thread_id]['traceback'] = traceback.format_exc()
                running_threads[thread_id]['end_time'] = time.time()

def run_rollup_script_in_background(company_id, thread_id):
    """Run the rollup script in a background thread"""
    try:
        with thread_lock:
            running_threads[thread_id] = {
                'status': 'running',
                'company_id': company_id,
                'start_time': time.time(),
                'type': 'rollup'
            }
        
        logger.info(f"Starting rollup script for company_id: {company_id}")
        result = rollcontroller.main(company_id=company_id)
        
        # Update thread status
        with thread_lock:
            if thread_id in running_threads:
                running_threads[thread_id]['status'] = 'completed'
                running_threads[thread_id]['result'] = result
                running_threads[thread_id]['end_time'] = time.time()
        
        logger.info("Rollup script completed successfully")
        
    except Exception as e:
        logger.error(f"Error in background rollup: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Update thread status with error
        with thread_lock:
            if thread_id in running_threads:
                running_threads[thread_id]['status'] = 'error'
                running_threads[thread_id]['error'] = str(e)
                running_threads[thread_id]['traceback'] = traceback.format_exc()
                running_threads[thread_id]['end_time'] = time.time()

# Root route to handle health checks
@app.route('/', methods=['GET'])
def root():
    """Root endpoint - redirects to health check"""
    return jsonify({
        'service': 'aggregation-rollup-service',
        'status': 'running',
        'endpoints': {
            'health': '/health',
            'aggregation': '/run-aggregation',
            'rollup': '/start-rollup',
            'rollup_api': '/api/rollup',
            'rollup_status': '/api/rollup/status',
            'rollup_data': '/api/rollup/data',
            'status': '/status/<thread_id>'
        }
    }), 200

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check if API is accessible
        from RegionAPI import fetch_all_company_safe
        companies = fetch_all_company_safe()
        
        return jsonify({
            'status': 'healthy',
            'message': 'Service is running',
            'api_status': 'connected' if companies else 'disconnected',
            'active_threads': len([t for t_id, t in running_threads.items() if t['status'] == 'running'])
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'degraded',
            'message': f'Service is running but encountered an error: {str(e)}',
            'active_threads': len([t for t_id, t in running_threads.items() if t['status'] == 'running'])
        }), 200
 
# New Rollup API Endpoints
@app.route('/api/rollup', methods=['POST'])
def api_rollup():
    """Enhanced rollup API endpoint with detailed parameters"""
    try:
        data = request.get_json() or {}
        
        # Extract parameters
        company_id = data.get('company_id')
        year = data.get('year')
        internal_code_id = data.get('internal_code_id')
        frequency = data.get('frequency', 'yearly')
        process_all = data.get('process_all', False)
        
        logger.info(f"Received rollup API request: company_id={company_id}, year={year}, internal_code_id={internal_code_id}, frequency={frequency}, process_all={process_all}")
        
        # Validate required parameters
        if not process_all and not company_id:
            return jsonify({
                'status': 'error',
                'error': 'company_id is required unless process_all is true'
            }), 400
        
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        
        # Initialize rollup controller
        try:
            controller = rollcontroller.SiteDataRollup()
        except Exception as e:
            logger.error(f"Failed to initialize rollup controller: {str(e)}")
            return jsonify({
                'status': 'error',
                'error': f'Failed to initialize rollup controller: {str(e)}'
            }), 500
        
        # Process rollup based on parameters
        if process_all:
            # Process all companies
            result = controller.process_company_data(company_id=None)
        elif year and internal_code_id:
            # Process specific year and internal_code_id for a company
            if not company_id:
                return jsonify({
                    'status': 'error',
                    'error': 'company_id is required when processing specific year and internal_code_id'
                }), 400
            
            # Fetch site data
            site_data = controller.fetch_site_data(str(company_id))
            if not site_data:
                return jsonify({
                    'status': 'error',
                    'error': f'Could not fetch site data for company {company_id}'
                }), 404
            
            # Get cdata based on frequency
            if frequency == 'monthly':
                cdata_list = list(controller.cdata_monthly.find({
                    "company_id": str(company_id),
                    "reporting_year": year,
                    "internal_code_id": internal_code_id
                }))
            elif frequency == 'quarterly':
                cdata_list = list(controller.cdata_quarterly.find({
                    "company_id": str(company_id),
                    "reporting_year": year,
                    "internal_code_id": internal_code_id
                }))
            elif frequency == 'bi_annual':
                cdata_list = list(controller.cdata_bi_annual.find({
                    "company_id": str(company_id),
                    "reporting_year": year,
                    "internal_code_id": internal_code_id
                }))
            else:  # yearly
                cdata_list = list(controller.cdata_yearly.find({
                    "company_id": str(company_id),
                    "reporting_year": year,
                    "internal_code_id": internal_code_id
                }))
            
            if not cdata_list:
                return jsonify({
                    'status': 'warning',
                    'message': f'No {frequency} data found for company {company_id}, year {year}, internal_code_id {internal_code_id}'
                }), 200
            
            # Process rollup
            controller.process_rollup(site_data, cdata_list, year, internal_code_id, frequency)
            
            result = {
                'success': True,
                'message': f'Rollup processed successfully for {frequency} data',
                'data': {
                    'company_id': company_id,
                    'year': year,
                    'internal_code_id': internal_code_id,
                    'frequency': frequency,
                    'records_processed': len(controller.new_rollup_table),
                    'rollup_summary': controller.export_to_database_format()
                }
            }
        else:
            # Process company with default logic
            result = controller.process_company_data(company_id=company_id)
        
        # Convert ObjectIds to strings for JSON serialization
        result = convert_objectids_to_strings(result)
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error in rollup API: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
 
@app.route('/api/rollup/status', methods=['GET'])
def api_rollup_status():
    """Get rollup processing status and statistics"""
    try:
        # Get query parameters
        company_id = request.args.get('company_id')
        frequency = request.args.get('frequency', 'all')
        
        # Initialize controller to access collections
        controller = rollcontroller.SiteDataRollup()
        
        # Get collection counts
        status_data = {}
        
        if frequency == 'all' or frequency == 'monthly':
            monthly_count = controller.rollup_monthly.count_documents({})
            status_data['monthly'] = monthly_count
        
        if frequency == 'all' or frequency == 'quarterly':
            quarterly_count = controller.rollup_quarterly.count_documents({})
            status_data['quarterly'] = quarterly_count
        
        if frequency == 'all' or frequency == 'bi_annual':
            bi_annual_count = controller.rollup_bi_annual.count_documents({})
            status_data['bi_annual'] = bi_annual_count
        
        if frequency == 'all' or frequency == 'yearly':
            yearly_count = controller.rollup_yearly.count_documents({})
            status_data['yearly'] = yearly_count
        
        # Get company-specific counts if company_id provided
        if company_id:
            company_filter = {"company_id": str(company_id)}
            status_data['company_specific'] = {}
            
            if frequency == 'all' or frequency == 'monthly':
                status_data['company_specific']['monthly'] = controller.rollup_monthly.count_documents(company_filter)
            if frequency == 'all' or frequency == 'quarterly':
                status_data['company_specific']['quarterly'] = controller.rollup_quarterly.count_documents(company_filter)
            if frequency == 'all' or frequency == 'bi_annual':
                status_data['company_specific']['bi_annual'] = controller.rollup_bi_annual.count_documents(company_filter)
            if frequency == 'all' or frequency == 'yearly':
                status_data['company_specific']['yearly'] = controller.rollup_yearly.count_documents(company_filter)
        
        return jsonify({
            'status': 'success',
            'data': status_data,
            'timestamp': time.time()
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting rollup status: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500
 
@app.route('/api/rollup/data', methods=['GET'])
def api_rollup_data():
    """Get rollup data with filtering options"""
    try:
        # Get query parameters
        company_id = request.args.get('company_id')
        frequency = request.args.get('frequency', 'yearly')
        year = request.args.get('year')
        internal_code_id = request.args.get('internal_code_id')
        limit = request.args.get('limit', 100, type=int)
        skip = request.args.get('skip', 0, type=int)
        
        # Initialize controller
        controller = rollcontroller.SiteDataRollup()
        
        # Build filter
        filter_query = {}
        if company_id:
            filter_query['company_id'] = str(company_id)
        if year:
            filter_query['type_year'] = int(year)
        if internal_code_id:
            filter_query['internal_code_id'] = internal_code_id
        
        # Select collection based on frequency
        if frequency == 'monthly':
            collection = controller.rollup_monthly
        elif frequency == 'quarterly':
            collection = controller.rollup_quarterly
        elif frequency == 'bi_annual':
            collection = controller.rollup_bi_annual
        else:  # yearly
            collection = controller.rollup_yearly
        
        # Get data with pagination
        cursor = collection.find(filter_query).skip(skip).limit(limit)
        data = list(cursor)
        
        # Convert ObjectIds to strings
        data = convert_objectids_to_strings(data)
        
        # Get total count
        total_count = collection.count_documents(filter_query)
        
        return jsonify({
            'status': 'success',
            'data': {
                'records': data,
                'pagination': {
                    'total': total_count,
                    'limit': limit,
                    'skip': skip,
                    'has_more': (skip + limit) < total_count
                },
                'filters': {
                    'company_id': company_id,
                    'frequency': frequency,
                    'year': year,
                    'internal_code_id': internal_code_id
                }
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting rollup data: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500
 
@app.route('/api/rollup/sites/<company_id>', methods=['GET'])
def api_rollup_sites(company_id):
    """Get site hierarchy for a specific company"""
    try:
        # Initialize controller
        controller = rollcontroller.SiteDataRollup()
        
        # Fetch site data
        site_data = controller.fetch_site_data(company_id)
        
        if not site_data:
            return jsonify({
                'status': 'error',
                'error': f'Could not fetch site data for company {company_id}'
            }), 404
        
        # Convert ObjectIds to strings
        site_data = convert_objectids_to_strings(site_data)
        
        return jsonify({
            'status': 'success',
            'data': {
                'company_id': company_id,
                'site_hierarchy': site_data
            }
        }), 200
        
    except Exception as e:
        logger.error(f"Error fetching site data: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500
 
@app.route('/run-aggregation', methods=['POST'])
def run_aggregation():
    """Run aggregation process in background"""
    try:
        # Get company_id from request
        company_id = None
        if request.json:
            company_id = request.json.get('company_id')
        if not company_id and request.args:
            company_id = request.args.get('company_id')
            
        logger.info(f"Received aggregation request for company_id: {company_id}")
        
        # Validate company_id if provided
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        
        # Check if there's already a running aggregation process for this company
        with thread_lock:
            for thread_id, thread_info in running_threads.items():
                if (thread_info['status'] == 'running' and 
                    thread_info['company_id'] == company_id and
                    thread_info['type'] == 'aggregation'):
                    return jsonify({
                        'status': 'already_running',
                        'message': f'Aggregation for company_id {company_id} is already running.',
                        'thread_id': thread_id
                    }), 409
        
        # Generate unique thread ID
        thread_id = f"agg_{company_id or 'all'}_{int(time.time())}"
        
        # Run script in background thread
        thread = threading.Thread(
            target=run_aggregation_script_in_background, 
            args=(company_id, thread_id),
            name=f"AggregationThread-{thread_id}"
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Aggregation process has been started in the background.',
            'company_id': company_id,
            'thread_id': thread_id
        }), 202
        
    except Exception as e:
        logger.error(f"Error triggering background aggregation: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/start-rollup', methods=['POST'])
def run_rollup():
    """Run rollup process in background"""
    try:
        # Get company_id from request
        company_id = None
        if request.json:
            company_id = request.json.get('company_id')
        if not company_id and request.args:
            company_id = request.args.get('company_id')
            
        logger.info(f"Received rollup request for company_id: {company_id}")
        
        # Validate company_id if provided
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        
        # Check if there's already a running rollup process for this company
        with thread_lock:
            for thread_id, thread_info in running_threads.items():
                if (thread_info['status'] == 'running' and 
                    thread_info['company_id'] == company_id and
                    thread_info['type'] == 'rollup'):
                    return jsonify({
                        'status': 'already_running',
                        'message': f'Rollup for company_id {company_id} is already running.',
                        'thread_id': thread_id
                    }), 409
        
        # Generate unique thread ID
        thread_id = f"rollup_{company_id or 'all'}_{int(time.time())}"
        
        # Run script in background thread
        thread = threading.Thread(
            target=run_rollup_script_in_background, 
            args=(company_id, thread_id),
            name=f"RollupThread-{thread_id}"
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'status': 'started',
            'message': 'Rollup process has been started in the background.',
            'company_id': company_id,
            'thread_id': thread_id
        }), 202
        
    except Exception as e:
        logger.error(f"Error triggering background rollup: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/run-aggregation/<int:company_id>', methods=['POST'])
def run_aggregation_for_company(company_id):
    """Run aggregation process synchronously for a specific company"""
    try:
        from main import CompanyDataController
        controller = CompanyDataController()
        result = controller.process_company_data(company_id=company_id)
        
        if result["success"]:
            logger.info(result["message"])
            return jsonify({
                'status': 'success',
                'message': result["message"],
                'company_id': company_id,
                'data': result.get('data', {})
            }), 200
        else:
            logger.error(result["message"])
            return jsonify({
                'status': 'error',
                'error': result["message"],
                'company_id': company_id
            }), 500
            
    except Exception as e:
        logger.error(f"Error running aggregation for company_id {company_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e),
            'company_id': company_id
        }), 500

@app.route('/run-rollup/<int:company_id>', methods=['POST'])
def run_rollup_for_company(company_id):
    """Run rollup process synchronously for a specific company"""
    try:
        result = rollcontroller.main(company_id)
        
        logger.info(f"Rollup completed for company_id {company_id}")
        return jsonify({
            'status': 'success',
            'message': f'Rollup completed for company_id {company_id}',
            'company_id': company_id,
            'result': result
        }), 200
            
    except Exception as e:
        logger.error(f"Error running rollup for company_id {company_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'error': str(e),
            'company_id': company_id
        }), 500

@app.route('/status/<thread_id>', methods=['GET'])
def get_status(thread_id):
    """Get the status of a specific process (aggregation or rollup)"""
    try:
        with thread_lock:
            if thread_id not in running_threads:
                return jsonify({
                    'status': 'not_found',
                    'error': 'Thread ID not found'
                }), 404
            
            thread_info = running_threads[thread_id].copy()
            
            # Convert ObjectId fields to strings
            thread_info = convert_objectids_to_strings(thread_info)
            
            # Calculate duration if available
            if 'end_time' in thread_info:
                thread_info['duration'] = thread_info['end_time'] - thread_info['start_time']
            elif thread_info['status'] == 'running':
                thread_info['duration'] = time.time() - thread_info['start_time']
        
        return jsonify({
            'status': 'success',
            'thread_info': thread_info
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting process status: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/list-threads', methods=['GET'])
def list_threads():
    """List all active and recent threads"""
    try:
        with thread_lock:
            threads = convert_objectids_to_strings(running_threads.copy())
            
            # Add duration calculation for each thread
            for thread_id, thread_info in threads.items():
                if 'end_time' in thread_info:
                    thread_info['duration'] = thread_info['end_time'] - thread_info['start_time']
                elif thread_info['status'] == 'running':
                    thread_info['duration'] = time.time() - thread_info['start_time']
        
        return jsonify({
            'status': 'success',
            'threads': threads,
            'count': len(threads)
        }), 200
        
    except Exception as e:
        logger.error(f"Error listing threads: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)