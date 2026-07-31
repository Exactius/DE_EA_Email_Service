import os
import uvicorn
from fastapi import FastAPI
from src.core.process_every_action_data import router as process_router
from src.utils.log_setup import setup_logging
from src.utils.trace import trace_middleware

setup_logging()

app = FastAPI(title="Every Action Email Service")
app.middleware("http")(trace_middleware)

app.include_router(process_router)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_config=None,
    )