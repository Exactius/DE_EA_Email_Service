$headers = @{
    "Content-Type" = "application/json"
}

$body = @{
    "project_id" = "ex-whitestork"
    "dataset_name" = "staging"
    "table_name" = "every_action_data_staging_email_test"
    "partner" = "whitestork"
    "email_to" = "data.analysis@exacti.us"
    "email_name_search_key" = "EveryAction Scheduled Report - Exactius_Contribution_Report - whitestork"
    "is_link" = $true
} | ConvertTo-Json

Write-Host "Making request..."
$response = Invoke-RestMethod -Uri "http://127.0.0.1:8000/" -Method Post -Headers $headers -Body $body
Write-Host "Response:"
$response | ConvertTo-Json 