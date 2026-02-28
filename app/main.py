from fastapi import FastAPI
from app.api.v1.endpoints import ingestion, delivery  # Existing (Files)
from app.api.v1.endpoints import stream     # NEW (Real-Time JSON)

app = FastAPI(title="InsureBackend Enterprise API")

# 1. Register Batch Ingestion Router (File Uploads)
# URL: POST /api/v1/additions
app.include_router(ingestion.router, prefix="/api/v1", tags=["Batch Ingestion"])

# 2. Register Stream Router (Real-Time JSON)
# URL: POST /api/v1/stream/add
app.include_router(stream.router, prefix="/api/v1/stream", tags=["Real-Time Stream"])
# Wire up the new delivery router
app.include_router(delivery.router, prefix="/api/v1/delivery", tags=["Outbound Delivery"])
@app.get("/")
def health_check():
    return {"status": "active", "system": "InsureBackend", "mode": "Multi-Tenant"}

if __name__ == "__main__":
    import uvicorn
    # Note: When running in production, remove reload=True
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)