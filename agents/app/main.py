from fastapi import FastAPI
from app.routers import lead_ingestion_agent, lead_scoring_agent, active_outreach_agent

app = FastAPI()

# Include the routers
app.include_router(lead_ingestion_agent.router, prefix="/api", tags=["Lead Ingestion Agent"])
app.include_router(lead_scoring_agent.router, prefix="/api", tags=["Lead Scoring Agent"])
app.include_router(active_outreach_agent.router, prefix="/api", tags=["Active Outreach Agent"])

@app.get("/")
def read_root():
    return {"message": "Welcome to the API!"}