from flask import Flask, jsonify, request
from flask_cors import CORS
import subprocess
import subprocess
import logging
import sys
import os
import threading
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
script_path = r"D:\\spectreco\\sarima-script\\aggregation\\main.py"

def run_script_in_background(company_id):
    try:
        cmd_args = [sys.executable, script_path]
        logger.info(f"company data===============>>>>>>: {cmd_args}")
        if company_id is not None:
            cmd_args.extend(['--company_id', str(company_id)])
            logger.info(f"Starting aggregation script for company_id: {company_id}")
        else:
            logger.info("Starting aggregation script for all companies")

        logger.info(f"Executing command: {' '.join(cmd_args)}")

        result = subprocess.run(
            cmd_args,
            cwd=os.path.dirname(script_path),
            capture_output=True,
            text=True,
            timeout=3600
        )

        if result.returncode == 0:
            logger.info("Aggregation script completed successfully")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Aggregation failed with code {result.returncode}")
            logger.error(f"Stderr: {result.stderr}")
    except subprocess.TimeoutExpired:
        logger.error("Aggregation script timed out")
    except Exception as e:
        logger.error(f"Error in background aggregation: {str(e)}")


@app.route('/run-aggregation', methods=['POST'])
def run_aggregation():
    try:
        company_id = request.json.get('company_id') if request.json else None
        company_id = company_id or request.args.get('company_id')
        logger.info(f"company data: {company_id}")
        if company_id:
            try:
                company_id = int(company_id)
            except ValueError:
                return jsonify({
                    'status': 'error',
                    'error': 'Invalid company_id. Must be an integer.'
                }), 400
        else:
            company_id = None

        # Run script in background thread
        thread = threading.Thread(target=run_script_in_background, args=(company_id,))
        thread.start()

        return jsonify({
            'status': 'started',
            'message': 'Aggregation process has been started in the background.',
            'company_id': company_id
        }), 202

    except Exception as e:
        logger.error(f"Error triggering background aggregation: {str(e)}")
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