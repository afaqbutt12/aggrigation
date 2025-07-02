from flask import Flask, jsonify, request
from flask_cors import CORS
import subprocess
import logging
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/run-aggregation', methods=['POST'])
def run_aggregation():
    try:
        # Get the path to the script
        script_path = os.path.join("/home/ubuntu/var/www/sarima-script/aggregation","main.py")
        
        logger.info(f"Starting aggregation script at {script_path}")
        
        # Run the script using the same Python interpreter
        result = subprocess.run(
            ['python3', script_path],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("Aggregation script completed successfully")
            return jsonify({
                'status': 'success',
                'output': result.stdout
            }), 200
        else:
            logger.error(f"Aggregation script failed: {result.stderr}")
            return jsonify({
                'status': 'error',
                'error': result.stderr
            }), 500
            
    except Exception as e:
        logger.error(f"Error running aggregation: {str(e)}")
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
