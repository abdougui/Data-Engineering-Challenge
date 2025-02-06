from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
import yaml
from sqlalchemy import create_engine
from db_loader import calculate_carbon_emissions

app = FastAPI()

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

engine = create_engine(
    f"postgresql+psycopg2://{config['database']['user']}:{config['database']['password']}@"
    f"{config['database']['host']}:{config['database']['port']}/{config['database']['dbname']}"
)

class EmissionResponse(BaseModel):
    month: str
    renewable_emissions: float
    coal_emissions: float
    gas_emissions: float
    nuclear_emissions: float
    oil_emissions: float

@app.get("/emissions", response_model=list[EmissionResponse])
def get_emissions(month: int = Query(None, ge=1, le=12), year: int = Query(None, ge=2000)):
    monthly_emissions = calculate_carbon_emissions(engine)
    monthly_emissions['month'] = monthly_emissions['month'].astype(str)
    
    filtered = monthly_emissions.copy()
    if year:
        filtered = filtered[filtered['month'].str.startswith(str(year))]
    if month:
        filtered = filtered[filtered['month'].str.contains(f"-{month:02d}")]
    
    if filtered.empty:
        raise HTTPException(status_code=404, detail="No emission data found for specified filters.")
    return filtered.to_dict(orient='records')

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
