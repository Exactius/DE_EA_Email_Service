"""
Data type definitions for BigQuery tables
"""

# Data types for CRM data
crm_dtypes = {
    'id': 'STRING',
    'first_name': 'STRING',
    'last_name': 'STRING',
    'email': 'STRING',
    'phone': 'STRING',
    'address': 'STRING',
    'city': 'STRING',
    'state': 'STRING',
    'zip': 'STRING',
    'country': 'STRING',
    'processed_at': 'TIMESTAMP',
    'partner': 'STRING',
    'email_name_search_key': 'STRING'
}

# Data types for regular data
dtypes = {
    'id': 'STRING',
    'email': 'STRING',
    'phone': 'STRING',
    'processed_at': 'TIMESTAMP',
    'partner': 'STRING',
    'email_name_search_key': 'STRING'
} 