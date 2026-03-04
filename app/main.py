from fastapi import FastAPI
from app.api.v1.endpoints import ingestion, delivery
from app.api.v1.endpoints import stream
from app.api.v1.endpoints import logs
from app.api.v1.endpoints import insurer_callbacks
from app.api.v1.endpoints import broker_admin

app = FastAPI(title="InsureBackend Enterprise API")

# 1. Register Batch Ingestion Router (File Uploads)
# URL: POST /api/v1/additions
app.include_router(ingestion.router, prefix="/api/v1", tags=["Batch Ingestion"])

# 2. Register Stream Router (Real-Time JSON)
app.include_router(stream.router, prefix="/api/v1/stream", tags=["Real-Time Stream"])
# Wire up the delivery router
app.include_router(delivery.router, prefix="/api/v1/delivery", tags=["Outbound Delivery"])

# Register logs & Audit router
app.include_router(logs.router, prefix="/api/v1/logs", tags=["Logs & Audit"])

app.include_router(insurer_callbacks.router, prefix="/api/v1", tags=["Insurer Callbacks"])
app.include_router(broker_admin.router, prefix="/api/v1/broker", tags=["Broker Admin"])

@app.get("/")
def health_check():
    return {"status": "active", "system": "InsureBackend", "mode": "Multi-Tenant"}

if __name__ == "__main__":
    import uvicorn
    # Note: When running in production, remove reload=True
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)