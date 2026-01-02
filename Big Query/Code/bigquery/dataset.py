from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_silver_dataset(project_id, dataset_id='silver_layer', location='US'):
    """Create BigQuery dataset for silver layer if it doesn't exist"""
    
    # Initialize BigQuery client
    bq_client = bigquery.Client(project=project_id)
    
    dataset_ref = f"{project_id}.{dataset_id}"
    
    try:
        # Check if dataset already exists
        bq_client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists")
        
    except Exception:
        # Create dataset if it doesn't exist
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = "Silver layer - cleaned and transformed data"
        
        bq_client.create_dataset(dataset)
        logger.info(f"Successfully created dataset {dataset_ref}")


if __name__ == "__main__":
    PROJECT_ID = 'project-cbe8701e-25df-447d-9da'
    
    create_silver_dataset(
        project_id=PROJECT_ID,
        dataset_id='silver_layer',
        location='US'
    )