# Test Data Directory

This directory contains Excel files for testing the data processing pipeline.

## File Structure

Place your Excel files in this directory. The Excel files should have the following columns:

### For Regular Data
- `id` (required): Unique identifier for each record
- `name` (optional): Name of the record
- `email` (optional): Email address
- `phone` (optional): Phone number
- `country` (optional): Country code
- Any additional columns specific to your data

### For Link Data
- `id` (required): Unique identifier for each record
- `url` (required): The link URL
- `title` (optional): Link title
- `description` (optional): Link description
- Any additional columns specific to your data

## Example Files

1. `sample_data.xlsx` - Example of regular data
2. `sample_links.xlsx` - Example of link data

## Usage

To test with an Excel file:

1. Place your Excel file in this directory
2. Use the `/test` endpoint with the file path:
   ```bash
   curl -X POST "http://localhost:8080/test" \
     -H "accept: application/json" \
     -H "Content-Type: multipart/form-data" \
     -F "file=@test_data/your_file.xlsx" \
     -F "request={\"project_id\":\"your-project-id\",\"dataset_name\":\"your-dataset\",\"table_name\":\"your-table\",\"partner\":\"test-partner\",\"email_name_search_key\":\"test-key\",\"is_link\":false}"
   ```

## Notes

- Make sure your Excel files follow the required column structure
- The `id` column is required and must be unique
- All dates should be in a format that pandas can parse
- Avoid using special characters in column names 