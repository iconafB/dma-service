from fastapi import FastAPI
from routes.dma_routes import dma_routes
from database.database import create_db_and_tables

#this will not happen here but in another service
#schedule the credits checks to run everyday in the morning

app=FastAPI(
    title="DMA SERVICE",
    summary="DMA SERVICE for interacting with the DMASA SERVICE",
    description="This DMA service will interact with the DMASA API. The DMA service main functions is to check dma credits, upload data for deduping,check dedupe status and read dedupe output. The check credits functionality is automated and update the frontend about the remaining number of credits.The upload data for deduping functionality will upload files to dmasa and the dedupe status will be continuously checked against a dmasa endpoint. Finally the files are read from the dmasa api endpoint provided they have been deduped successfully."
)

@app.get("/dma")

async def dma_main():
    return {"main":"main dna service test endpoint"}

@app.get("/health-check")

async def health_check():
    return {"main":"dma health check"}

app.include_router(dma_routes)

create_db_and_tables()



