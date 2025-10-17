from fastapi import FastAPI, HTTPException
from typing import Dict, Optional
import json
import uvicorn

app = FastAPI()

# In-memory storage for schemas
schemas: Dict[str, Dict] = {}

@app.post("/subjects/{subject}/versions")
async def register_schema(subject: str, schema: Dict):
    try:
        # Validate schema
        if not isinstance(schema, dict) or 'schema' not in schema:
            raise HTTPException(status_code=400, message="Invalid schema format")
        
        if subject not in schemas:
            schemas[subject] = []
        
        # Add version
        version = len(schemas[subject]) + 1
        schema_entry = {
            'version': version,
            'schema': schema['schema']
        }
        schemas[subject].append(schema_entry)
        
        return {
            'id': f"{subject}-v{version}",
            'version': version,
            'subject': subject
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/subjects/{subject}/versions/{version}")
async def get_schema(subject: str, version: str):
    try:
        if subject not in schemas:
            raise HTTPException(status_code=404, detail="Subject not found")
        
        if version == "latest":
            schema_entry = schemas[subject][-1]
        else:
            version_num = int(version)
            if version_num < 1 or version_num > len(schemas[subject]):
                raise HTTPException(status_code=404, detail="Version not found")
            schema_entry = schemas[subject][version_num - 1]
        
        return {
            'subject': subject,
            'version': schema_entry['version'],
            'schema': schema_entry['schema']
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def start_registry(host: str = "0.0.0.0", port: int = 8081):
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    start_registry()
