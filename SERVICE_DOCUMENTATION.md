# DE_EA_Email_Service - Complete Documentation

## What This Service Does

The **DE_EA_Email_Service** is a FastAPI service that processes EveryAction Contribution Reports and uploads them to BigQuery. It supports two data sources:

1. **Email Attachments** - Automatically fetches CSV attachments from Gmail
2. **Direct CSV Upload** - Manually upload CSV files

### Main Flow:

```
Gmail Email (with CSV attachment)
    ↓
Service fetches attachment via Gmail API
    ↓
Decode CSV (UTF-16 or UTF-8)
    ↓
Transform data (hash PII, rename columns, add metadata)
    ↓
Upload to BigQuery
    ↓
Delete email from Gmail
```

---

## How It Works - Detailed

### 1. **Email Processing** (source_type: "email")

**Entry Point:** `POST /process` with `source_type: "email"`

**Steps:**
1. **Authenticate** with Google Cloud (gets access token from service account)
2. **Search Gmail** using partner-specific subject line:
   - WhiteStork: `"subject:EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork"`
   - Exactius: `"subject:EveryAction Scheduled Report - Exactius_Contribution_Report - exactius"`
   - STYT: `"subject:EveryAction Scheduled Report - Exactius_Styt_Contribution_Report"`
3. **Download CSV attachment** from email
4. **Auto-detect encoding** (UTF-16 or UTF-8)
5. **Transform data:**
   - Hash sensitive fields: `email`, `phone`, `first_name`, `last_name` (SHA-256)
   - Rename columns to snake_case
   - Add metadata: `processed_at`, `partner`, `email_name_search_key`
6. **Upload to BigQuery** (WRITE_TRUNCATE mode)
7. **Delete email** from Gmail inbox

**Files Involved:**
- `src/core/process_every_action_data.py` - Main orchestration
- `src/services/email_service.py` - Gmail API integration
- `src/services/data_transformation_service.py` - Data transformation logic
- `src/services/bigquery_service.py` - BigQuery upload

---

### 2. **CSV Upload** (source_type: "csv")

**Entry Point:** `POST /process` with `source_type: "csv"`

**Steps:**
1. **Read CSV file** from `test_data/` folder
2. **Auto-detect encoding** (UTF-16 or UTF-8)
3. **Transform data** (same as email processing)
4. **Upload to BigQuery**
5. **No email deletion** (manual upload)

---

## Data Transformation Details

### Fields That Get Hashed (SHA-256):
- `Personal Email` → `email` (hashed)
- `Preferred Phone Number` → `phone` (hashed)
- `Contact Name` → split into `first_name` (hashed) and `last_name` (hashed)

### Fields That Get Renamed (Not Hashed):
- `Contribution ID` → `contribution_id`
- `VANID` → `vanid`
- `Date Received` → `date_received`
- `Amount` → `amount`
- `utm_campaign (Exactius)` → `utm_campaign`
- `utm_medium (Exactius)` → `utm_medium`
- `utm_source (Exactius)` → `utm_source`
- `Mailing Zip/Postal` → `mailing_zip` *(NEW - Nov 15, 2025)*
- *(+40 more fields - see full mapping in code)*

### Fields That Get Added:
- `processed_at` - Timestamp when processed
- `partner` - Partner name (whitestork, exactius, styt)
- `email_name_search_key` - Email search query used
- `id` - Unique identifier (uses contribution_id)

---

## Common Errors & Fixes

### Error 1: `Unknown partner: whitestrok`

**Error Message:**
```
HTTPException: Unknown partner: whitestrok
```

**Cause:** Typo in partner name

**Fix:**
```json
// WRONG:
"partner": "whitestrok"

// CORRECT:
"partner": "whitestork"  // ← Add the 'c'
```

**Valid Partners:** `whitestork`, `exactius`, `styt`

---

### Error 2: `UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff`

**Error Message:**
```
UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position 0
```

**Cause:** CSV file is UTF-16 encoded, but old code only supported UTF-8

**Fix:**
- ✅ **FIXED in latest version** (Nov 15, 2025)
- Auto-detects UTF-16 or UTF-8 encoding
- Redeploy service to get the fix

**Verify Fix:**
```bash
curl https://ea-email-service-815522637671.europe-west2.run.app/health
# Should return: {"status":"healthy"}
```

---

### Error 3: `No data found in attachment`

**Error Message:**
```json
{"status": "error", "message": "No data found in attachment"}
```

**Cause:** Email not found or wrong subject line

**Fix:**
1. **Check Gmail** - Is the email in the inbox?
2. **Verify subject line** matches exactly:
   - WhiteStork: `EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork`
   - Exactius: `EveryAction Scheduled Report - Exactius_Contribution_Report - exactius`
   - STYT: `EveryAction Scheduled Report - Exactius_Styt_Contribution_Report`
3. **Check email recipient:** Must be sent to `data.analysis@exacti.us`

---

### Error 4: `CSV file not found: ContributionReport-xxx.csv`

**Error Message:**
```
HTTPException: CSV file not found: ContributionReport-2502759392.csv
```

**Cause:** CSV file not in `test_data/` folder

**Fix:**
```bash
# Place CSV in the correct folder
cp /path/to/your/file.csv DE_EA_Email_Service/test_data/

# Or use full path in request:
"csv_filename": "ContributionReport-2502759392.csv"
```

---

### Error 5: `Failed to upload to BigQuery`

**Error Message:**
```
Failed to upload to BigQuery: 403 Forbidden
```

**Cause:** Service account lacks BigQuery permissions

**Fix:**
1. **Check service account** has these roles:
   - `BigQuery Data Editor`
   - `BigQuery Job User`
2. **Verify project ID** is correct
3. **Check dataset exists** in BigQuery

---

## How to Test in Postman

### Test 1: Health Check

**Method:** `GET`
**URL:** `https://ea-email-service-815522637671.europe-west2.run.app/health`

**Expected Response:**
```json
{
  "status": "healthy"
}
```

---

### Test 2: Email Processing (WhiteStork)

**Method:** `POST`
**URL:** `https://ea-email-service-815522637671.europe-west2.run.app/process`
**Headers:**
```
Content-Type: application/json
```

**Body (raw JSON):**
```json
{
  "partner": "whitestork",
  "project_id": "ex-whitestork",
  "dataset": "staging",
  "table_name": "every_action_contribution_report_test",
  "source_type": "email"
}
```

**Expected Response:**
```json
{
  "status": "success",
  "message": "Successfully processed and uploaded 1234 rows"
}
```

---

### Test 3: Email Processing (Exactius)

**Method:** `POST`
**URL:** `https://ea-email-service-815522637671.europe-west2.run.app/process`
**Headers:**
```
Content-Type: application/json
```

**Body (raw JSON):**
```json
{
  "partner": "exactius",
  "project_id": "ex-omaze-de",
  "dataset": "staging",
  "table_name": "every_action_contribution_report_test",
  "source_type": "email"
}
```

---

### Test 4: Email Processing (STYT)

**Method:** `POST`
**URL:** `https://ea-email-service-815522637671.europe-west2.run.app/process`
**Headers:**
```
Content-Type: application/json
```

**Body (raw JSON):**
```json
{
  "partner": "styt",
  "project_id": "ex-styt",
  "dataset": "staging",
  "table_name": "every_action_contribution_report_test",
  "source_type": "email"
}
```

---

### Test 5: CSV Upload (Manual)

**Method:** `POST`
**URL:** `https://ea-email-service-815522637671.europe-west2.run.app/process`
**Headers:**
```
Content-Type: application/json
```

**Body (raw JSON):**
```json
{
  "partner": "whitestork",
  "project_id": "ex-whitestork",
  "dataset": "staging",
  "table_name": "csv_upload_test",
  "source_type": "csv",
  "csv_filename": "ContributionReport-2502759392.csv"
}
```

**Expected Response:**
```json
{
  "status": "success",
  "message": "Successfully processed and uploaded 1234 rows"
}
```

**Note:** CSV file must exist in `test_data/` folder on the server

---

## How to Run CSV Upload Locally

### Step 1: Place CSV File
```bash
cd DE_EA_Email_Service
# Copy your CSV to test_data folder
cp /path/to/ContributionReport-2502759392.csv test_data/
```

---

### Step 2: Start Local Service
```bash
cd DE_EA_Email_Service
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Expected Output:**
```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [12345] using StatReload
INFO:     Started server process [12346]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

---

### Step 3: Test Health Check
```bash
curl http://localhost:8000/health
```

**Expected:**
```json
{"status":"healthy"}
```

---

### Step 4: Upload CSV
```bash
curl -X POST "http://localhost:8000/process" \
  -H "Content-Type: application/json" \
  -d '{
    "partner": "whitestork",
    "project_id": "ex-whitestork",
    "dataset": "staging",
    "table_name": "csv_test_upload",
    "source_type": "csv",
    "csv_filename": "ContributionReport-2502759392.csv"
  }'
```

---

### Step 5: Check Logs

You'll see detailed logs in the terminal:
```
Detected encoding: utf-16
Columns from DataFrame:
['Contribution ID', 'VANID', 'Contact Name', 'Date Received', ...]

Renaming columns...
  Renaming: 'Contribution ID' -> 'contribution_id'
  Renaming: 'VANID' -> 'vanid'
  ...

Hashed email
Sample values for email:
0    a3f2b1c...
1    d4e5f6a...
...

Successfully uploaded 1234 rows to BigQuery
```

---

### Step 6: Verify in BigQuery

```sql
SELECT
  contribution_id,
  date_received,
  amount,
  partner,
  processed_at
FROM `ex-whitestork.staging.csv_test_upload`
ORDER BY processed_at DESC
LIMIT 10;
```

---

## Environment Variables Required

Create `.env` file in project root:
```bash
# Google Cloud
PROJECT_ID=ex-whitestork
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# BigQuery Defaults
DESTINATION_DATASET=staging
DESTINATION_TABLE=every_action_contribution_report

# Gmail
EMAIL_TO=data.analysis@exacti.us
```

---

## Service Endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/health` | GET | Health check |
| `/process` | POST | Process email or CSV |

---

## Partner Configurations

| Partner | Project ID | Email Subject Pattern |
|---------|-----------|----------------------|
| whitestork | ex-whitestork | `EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork` |
| exactius | ex-omaze-de | `EveryAction Scheduled Report - Exactius_Contribution_Report - exactius` |
| styt | ex-styt | `EveryAction Scheduled Report - Exactius_Styt_Contribution_Report` |

---

## Recent Changes (Nov 15, 2025)

### UTF-16 Encoding Support
- **Files Modified:** `email_service.py`, `process_every_action_data.py`
- **What Changed:** Auto-detects UTF-16 or UTF-8 encoding (previously only UTF-8)
- **Why:** EveryAction exports CSVs in UTF-16 encoding

### New Field Added
- **Field:** `Mailing Zip/Postal` → `mailing_zip`
- **Files Modified:** `process_every_action_data.py`, `data_transformation_service.py`
- **What Changed:** New field from EveryAction (47 columns instead of 46)
- **Backward Compatible:** Old CSVs (46 cols) still work

---

## Deployment

### Deploy to Cloud Run
```bash
cd DE_EA_Email_Service
gcloud run deploy ea-email-service \
  --source . \
  --region europe-west2 \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars PROJECT_ID=ex-whitestork
```

### Production URL
`https://ea-email-service-815522637671.europe-west2.run.app`

---

## File Structure

```
DE_EA_Email_Service/
├── main.py                          # FastAPI app entry point
├── src/
│   ├── core/
│   │   └── process_every_action_data.py  # Main orchestration
│   ├── services/
│   │   ├── email_service.py              # Gmail API integration
│   │   ├── data_transformation_service.py # Data transformation
│   │   ├── bigquery_service.py           # BigQuery upload
│   │   ├── auth_service.py               # Google Cloud auth
│   │   └── secret_service.py             # Secret Manager
│   └── utils/
│       ├── error_handling.py             # Error classes
│       └── validation.py                 # Data validation
├── test_data/                       # CSV files for testing
├── .env                             # Environment variables
└── requirements.txt                 # Python dependencies
```

---

## Quick Reference

### Valid Partner Names:
- `whitestork` ✅
- `exactius` ✅
- `styt` ✅
- `whitestrok` ❌ (typo - will fail)

### Valid Source Types:
- `email` - Fetch from Gmail
- `csv` - Upload from test_data/

### Required Fields in Request:
- `partner` (string)
- `project_id` (string)
- `dataset` (string)
- `table_name` (string)
- `source_type` (string: "email" or "csv")
- `csv_filename` (string - only if source_type="csv")

---

## Support

**Service Location:** `C:\Users\shai_exacti\Documents\try\every_action_data\DE_EA_Email_Service`
**Last Updated:** November 15, 2025
**Contact:** Data Engineering Team

---

**Need help?** Check the logs, verify partner name, and ensure CSV encoding is UTF-16 or UTF-8.
