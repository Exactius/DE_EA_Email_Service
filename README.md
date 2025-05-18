# Every Action Data Service

A service for processing and uploading Every Action data to BigQuery.

## Overview

This service processes email attachments from Every Action, transforms the data, and uploads it to BigQuery. It supports both link data and regular data processing, as well as direct CSV file uploads.

## Features

- Email attachment processing
- Direct CSV file upload support
- Data transformation and normalization
- BigQuery integration
- Secure secret management
- FastAPI-based REST API
- Automatic email cleanup after processing
- Sensitive data hashing
- Partner-specific configurations

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
export PROJECT_ID=your-project-id
export BIGQUERY_PROJECT_ID=your-project-id
```

3. Run the service:
```bash
python main.py
```

## API Endpoints

### 1. Process Data (`POST /process`)

Process data from either email attachment or direct CSV upload and upload to BigQuery.

**Request Body:**
```json
{
    "partner": "partner-name",  // Required: Partner identifier (e.g., "whitestork", "exactius", "styt")
    "project_id": "your-project-id",  // Required: Google Cloud project ID
    "dataset": "your-dataset",  // Required: BigQuery dataset name
    "table_name": "your-table",  // Required: BigQuery table name
    "is_link": false,  // Optional: Whether to process as link data
    "source_type": "email",  // Required: "email" or "csv"
    "csv_filename": "filename.csv"  // Required if source_type is "csv"
}
```

**Response:**
```json
{
    "status": "success",
    "message": "Data processed and uploaded successfully",
    "rows_processed": 100
}
```

### 2. Process CSV File (`POST /process-csv`)

Direct CSV file upload endpoint.

**Form Data:**
- `file`: CSV file to upload
- `partner`: Partner identifier
- `project_id`: Google Cloud project ID
- `dataset`: BigQuery dataset name
- `table_name`: BigQuery table name
- `is_link`: Whether to process as link data

## Partner Configurations

The service includes predefined configurations for different partners:

```python
PARTNER_CONFIGS = {
    'whitestork': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork"'
    },
    'exactius': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - exactius"'
    },
    'styt': {
        'email_name_search_key': 'subject:"EveryAction Scheduled Report - Exactius_Styt_Contribution_Report"'
    }
}
```

## Data Processing Flow

1. **Email Processing**:
   - Search for emails using partner-specific search keys
   - Extract attachments from matching emails
   - Process attachment data
   - Delete processed emails

2. **CSV Processing**:
   - Read CSV file with proper encoding
   - Handle SEP= line if present
   - Map columns to standardized names
   - Hash sensitive fields (email, phone, names)
   - Convert dates to MM/DD/YYYY format

3. **Data Transformation**:
   - Standardize column names
   - Hash sensitive data
   - Format dates
   - Handle missing values

4. **BigQuery Upload**:
   - Upload transformed data to specified table
   - Handle schema updates if needed
   - Log upload results

## Security Features

- Sensitive data hashing (SHA-256)
- Secure token management
- Google Secret Manager integration
- Automatic email cleanup
- Input validation and sanitization

## Error Handling

The service includes comprehensive error handling:
- HTTP exceptions with appropriate status codes
- Detailed error messages
- Logging of all operations
- Retry mechanisms for API calls

## Testing

Test files are located alongside their respective service files:
- `src/services/test_email_service.py`: Email service tests
- `src/services/test_services.py`: General service tests
- `src/core/test_request.py`: API endpoint tests

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 