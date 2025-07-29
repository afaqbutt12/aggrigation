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

def run__aggregation_script_in_background(company_id, thread_id):
    """Run the main script in a background thread"""
    try:
        with thread_lock:
            running_threads[thread_id] = {
                'status': 'running',
                'company_id': company_id,
                'start_time': time.time()
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
            
        logger.info(f"Received company_id: {company_id}")
        
        # Validate company_id if provided
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        
        # Check if there's already a running process for this company
        with thread_lock:
            for thread_id, thread_info in running_threads.items():
                if (thread_info['status'] == 'running' and 
                    thread_info['company_id'] == company_id):
                    return jsonify({
                        'status': 'already_running',
                        'message': f'Aggregation for company_id {company_id} is already running.',
                        'thread_id': thread_id
                    }), 409
        
        # Generate unique thread ID
        thread_id = f"agg_{company_id or 'all'}_{int(time.time())}"
        
        # Run script in background thread
        thread = threading.Thread(
            target=run__aggregation_script_in_background, 
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

@app.route('/aggregation-status/<thread_id>', methods=['GET'])
def get_aggregation_status(thread_id):
    """Get the status of a specific aggregation process"""
    try:
        with thread_lock:
            if thread_id not in running_threads:
                return jsonify({
                    'status': 'not_found',
                    'error': 'Thread ID not found'
                }), 404
            
            thread_info = running_threads[thread_id].copy()
            
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
        logger.error(f"Error getting aggregation status: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/aggregation-status', methods=['GET'])
def get_all_aggregation_status():
    """Get the status of all aggregation processes"""
    try:
        with thread_lock:
            all_threads = {}
            for thread_id, thread_info in running_threads.items():
                thread_copy = thread_info.copy()
                
                # Calculate duration
                if 'end_time' in thread_copy:
                    thread_copy['duration'] = thread_copy['end_time'] - thread_copy['start_time']
                elif thread_copy['status'] == 'running':
                    thread_copy['duration'] = time.time() - thread_copy['start_time']
                
                all_threads[thread_id] = thread_copy
        
        return jsonify({
            'status': 'success',
            'threads': all_threads,
            'total_threads': len(all_threads)
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting all aggregation status: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

def run__rollup_script_in_background(company_id, thread_id):
    """Run the main script in a background thread"""
    try:
        with thread_lock:
            running_threads[thread_id] = {
                'status': 'running',
                'company_id': company_id,
                'start_time': time.time()
            }
        
        logger.info(f"Starting aggregation script for company_id: {company_id}")
        result = rollcontroller.main(company_id=company_id)
        
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
            
        logger.info(f"Received company_id: {company_id}")
        
        # Validate company_id if provided
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        
        # Check if there's already a running process for this company
        with thread_lock:
            for thread_id, thread_info in running_threads.items():
                if (thread_info['status'] == 'running' and 
                    thread_info['company_id'] == company_id):
                    return jsonify({
                        'status': 'already_running',
                        'message': f'Aggregation for company_id {company_id} is already running.',
                        'thread_id': thread_id
                    }), 409
        
        # Generate unique thread ID
        thread_id = f"agg_{company_id or 'all'}_{int(time.time())}"
        
        # Run script in background thread
        thread = threading.Thread(
            target=run__rollup_script_in_background, 
            args=(company_id, thread_id),
            name=f"rollupThread-{thread_id}"
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

@app.route('/run-aggregation/<int:company_id>', methods=['POST'])
def run_rollup_for_company(company_id):
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

@app.route('/aggregation-status/<thread_id>', methods=['GET'])
def get_rollup_status(thread_id):
    """Get the status of a specific aggregation process"""
    try:
        with thread_lock:
            if thread_id not in running_threads:
                return jsonify({
                    'status': 'not_found',
                    'error': 'Thread ID not found'
                }), 404
            
            thread_info = running_threads[thread_id].copy()
            
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
        logger.error(f"Error getting aggregation status: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/aggregation-status', methods=['GET'])
def get_all_rollup_status():
    """Get the status of all aggregation processes"""
    try:
        with thread_lock:
            all_threads = {}
            for thread_id, thread_info in running_threads.items():
                thread_copy = thread_info.copy()
                
                # Calculate duration
                if 'end_time' in thread_copy:
                    thread_copy['duration'] = thread_copy['end_time'] - thread_copy['start_time']
                elif thread_copy['status'] == 'running':
                    thread_copy['duration'] = time.time() - thread_copy['start_time']
                
                all_threads[thread_id] = thread_copy
        
        return jsonify({
            'status': 'success',
            'threads': all_threads,
            'total_threads': len(all_threads)
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting all aggregation status: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
