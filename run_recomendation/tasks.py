from celery_app import celery
from company_codes_controller import CompanyCodesController
import logging

logger = logging.getLogger(__name__)

@celery.task(bind=True)
def merge_company_codes_task(self):
    """
    Background task to merge company codes.
    This task groups documents by company_id and internal_code_id,
    combining their site_code values into arrays.
    """
    try:
        # Update task state to PROGRESS
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Starting merge process...', 'progress': 0}
        )
        
        # Initialize controller
        controller = CompanyCodesController()
        
        # Update progress
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Connected to database, starting merge...', 'progress': 25}
        )
        
        # Perform the merge operation
        result = controller.merge_company_codes()
        
        # Close connection
        controller.close_connection()
        
        # Update progress
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Merge completed', 'progress': 100}
        )
        
        if result['status'] == 'success':
            return {
                'status': 'SUCCESS',
                'result': result
            }
        else:
            return {
                'status': 'FAILURE',
                'result': result
            }
            
    except Exception as e:
        logger.error(f"Task failed: {str(e)}")
        return {
            'status': 'FAILURE',
            'result': {
                'status': 'error',
                'message': f'Task execution failed: {str(e)}'
            }
        }

@celery.task(bind=True)
def get_collection_stats_task(self):
    """
    Background task to get collection statistics.
    """
    try:
        self.update_state(
            state='PROGRESS',
            meta={'status': 'Fetching collection statistics...', 'progress': 50}
        )
        
        controller = CompanyCodesController()
        result = controller.get_collection_stats()
        controller.close_connection()
        
        return {
            'status': 'SUCCESS',
            'result': result
        }
        
    except Exception as e:
        logger.error(f"Stats task failed: {str(e)}")
        return {
            'status': 'FAILURE',
            'result': {
                'status': 'error',
                'message': f'Failed to get statistics: {str(e)}'
            }
        }