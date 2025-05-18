import uvicorn
from src.core.process_every_action_data import app
import os

# This file just imports and uses the FastAPI app from process_every_action_data.py
# Run with: python -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload 

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port) 