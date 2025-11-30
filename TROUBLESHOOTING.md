# DE_EA_Email_Service - Troubleshooting Guide

## Recent Changes (Nov 15, 2025)

### UTF-16 Encoding Support + New Field
- **Auto-detect UTF-16/UTF-8** for email attachments and CSV uploads
- **New field:** `Mailing Zip/Postal` → `mailing_zip` in BigQuery
- **Backward compatible:** Old CSVs (46 cols) and new CSVs (47 cols) both work

---

## Quick Health Check

```bash
curl https://ea-email-service-815522637671.europe-west2.run.app/health
```
**Expected:** `{"status":"healthy"}`

---

## Test Calls (Postman/cURL)

### 1. Email Processing (WhiteStork)
```bash
curl -X POST "https://ea-email-service-815522637671.europe-west2.run.app/process" \
  -H "Content-Type: application/json" \
  -d '{
    "partner": "whitestork",
    "project_id": "ex-whitestork",
    "dataset": "staging",
    "table_name": "every_action_contribution_report_test",
    "source_type": "email"
  }'
```

### 2. Email Processing (Exactius)
```bash
curl -X POST "https://ea-email-service-815522637671.europe-west2.run.app/process" \
  -H "Content-Type: application/json" \
  -d '{
    "partner": "exactius",
    "project_id": "ex-omaze-de",
    "dataset": "staging",
    "table_name": "every_action_contribution_report_test",
    "source_type": "email"
  }'
```

### 3. Email Processing (STYT)
```bash
curl -X POST "https://ea-email-service-815522637671.europe-west2.run.app/process" \
  -H "Content-Type: application/json" \
  -d '{
    "partner": "styt",
    "project_id": "ex-styt",
    "dataset": "staging",
    "table_name": "every_action_contribution_report_test",
    "source_type": "email"
  }'
```

### 4. CSV Upload (Manual Test)
```bash
curl -X POST "https://ea-email-service-815522637671.europe-west2.run.app/process" \
  -H "Content-Type: application/json" \
  -d '{
    "partner": "whitestork",
    "project_id": "ex-whitestork",
    "dataset": "staging",
    "table_name": "csv_upload_test",
    "source_type": "csv",
    "csv_filename": "ContributionReport-2502759392.csv"
  }'
```

---

## Common Errors

| Error | Root Cause | Solution |
|-------|-----------|----------|
| `Unknown partner: whitestrok` | Typo in partner name | Use `"whitestork"` (with 'c') |
| `UnicodeDecodeError: 'utf-8'...` | Old code not deployed | Redeploy service with UTF-16 support |
| `CSV file not found` | File not in test_data/ | Place CSV in `test_data/` folder |
| `No data found in attachment` | Email not found or no CSV | Check email subject matches config |
| `Failed to upload to BigQuery` | BigQuery permissions | Check service account has access |

---

## Partner Configurations

```python
'whitestork': 'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork"'
'exactius':   'subject:"EveryAction Scheduled Report - Exactius_Contribution_Report - exactius"'
'styt':       'subject:"EveryAction Scheduled Report - Exactius_Styt_Contribution_Report"'
```

---

## Files Modified (Nov 15, 2025)

1. `src/services/email_service.py` - UTF-16 auto-detection for email attachments
2. `src/core/process_every_action_data.py` - UTF-16 auto-detection for CSV uploads + new field mapping
3. `src/services/data_transformation_service.py` - New field mapping for email processing

---

## Production Endpoints

- **Service URL:** `https://ea-email-service-815522637671.europe-west2.run.app`
- **Health:** `/health`
- **Process:** `/process` (POST)

---

## Local Testing

```bash
# Start service locally
cd DE_EA_Email_Service
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000

# Test health
curl http://localhost:8000/health

# Test CSV upload
curl -X POST "http://localhost:8000/process" \
  -H "Content-Type: application/json" \
  -d '{
    "partner": "whitestork",
    "project_id": "ex-whitestork",
    "dataset": "staging",
    "table_name": "test_table",
    "source_type": "csv",
    "csv_filename": "ContributionReport-2502759392.csv"
  }'
```

---

## Deployment

```bash
# Build and deploy to Cloud Run
gcloud run deploy ea-email-service \
  --source . \
  --region europe-west2 \
  --platform managed \
  --allow-unauthenticated
```

---

## Contact
- **Service:** DE_EA_Email_Service
- **Last Updated:** Nov 15, 2025
- **Changes:** UTF-16 support + Mailing Zip/Postal field
