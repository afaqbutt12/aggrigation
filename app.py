from flask import Flask, jsonify, request
from flask_cors import CORS
import subprocess
import logging
import sys
import os
from dotenv import load_dotenv
from main import CompanyDataController

# Load environment variables
load_dotenv()
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set your script path here
script_path = os.getenv('path', "D:\\spectreco\\sarima-script\\aggregation\\main.py")

@app.route('/run-aggregation', methods=['POST'])
def run_aggregation():
    try:
        company_id = request.json.get('company_id') or request.args.get('company_id')
        
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        
        cmd_args = [sys.executable, script_path]
        
        if company_id:
            cmd_args.extend(['--company_id', str(company_id)])
            logger.info(f"Starting aggregation script for company_id: {company_id}")
        else:
            logger.info("Starting aggregation script for all companies")
        
        logger.info(f"Executing command: {' '.join(cmd_args)}")
        
        result = subprocess.run(
            cmd_args,
            cwd=os.path.dirname(script_path),  # Set working directory
            capture_output=True,  # Capture stdout and stderr
            text=True,  # Return strings instead of bytes
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            logger.info("Aggregation script completed successfully")
            return jsonify({
                'status': 'success',
                'message': 'Aggregation completed successfully',
                'output': result.stdout,
                'company_id': company_id
            }), 200
        else:
            logger.error(f"Aggregation script failed with return code {result.returncode}")
            logger.error(f"Script stderr: {result.stderr}")
            return jsonify({
                'status': 'error',
                'error': result.stderr,
                'output': result.stdout,
                'return_code': result.returncode
            }), 500
            
    except subprocess.TimeoutExpired:
        logger.error("Aggregation script timed out")
        return jsonify({
            'status': 'error',
            'error': 'Script execution timed out'
        }), 500
    except Exception as e:
        logger.error(f"Error running aggregation: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

@app.route('/run-aggregation/<int:company_id>', methods=['POST'])
def run_aggregation_for_company(company_id):
    try:
        controller = CompanyDataController()
        result = controller.process_company_data(company_id=company_id)
        
        if result["success"]:
            logger.info(result["message"])
            return jsonify({
                'status': 'success',
                'message': result["message"],
                'company_id': company_id
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
        return jsonify({
            'status': 'error',
            'error': str(e),
            'company_id': company_id
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'message': 'Service is running'
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)