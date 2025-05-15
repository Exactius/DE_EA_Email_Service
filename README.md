# Every Action Data Service

A service for processing and uploading Every Action data to BigQuery.

## Overview

This service processes email attachments from Every Action, transforms the data, and uploads it to BigQuery. It supports both link data and regular data processing.

## Features

- Email attachment processing
- Data transformation and normalization
- BigQuery integration
- Secure secret management
- FastAPI-based REST API

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
export PROJECT_ID=your-project-id
```

3. Run the service:
```bash
python main.py
```

## API Usage

Send a POST request to the root endpoint with the following JSON body:

```json
{
    "project_id": "your-project-id",
    "dataset_name": "your-dataset",
    "table_name": "your-table",
    "partner": "partner-name",
    "email_name_search_key": "search-key",
    "email_to": "recipient@example.com",
    "is_link": true
}
```

## Service Structure

- `main.py`: FastAPI application entry point
- `services/`: Service modules
  - `auth_service.py`: Authentication and token management
  - `email_service.py`: Email processing
  - `bigquery_service.py`: BigQuery operations
  - `data_transformation_service.py`: Data processing
  - `secret_service.py`: Secret management

## Error Handling

The service includes comprehensive error handling and logging. All errors are caught and returned with appropriate status codes and messages.

## Security

- Access tokens are managed securely
- Secrets are stored in Google Secret Manager
- API endpoints are protected

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 