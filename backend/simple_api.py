from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from simple_query import query_database

app = FastAPI(title="Simple Customer Query API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    query: str

@app.get("/")
def health():
    return {"status": "Simple Customer Query API is running"}

@app.post("/query")
async def process_query(request: QueryRequest):
    """Process a natural language query and return results."""
    try:
        # Get the query result
        result = query_database(request.query)
        
        # If we got a dictionary response, handle it
        if isinstance(result, dict):
            # Ensure the response has all required fields
            if 'type' not in result:
                result['type'] = 'metric'  # Default type
            if 'label' not in result:
                result['label'] = 'Result'  # Default label
            if 'value' not in result and 'data' in result:
                # Try to extract a value if we have data
                if len(result['data']) > 0 and isinstance(result['data'][0], dict):
                    if 'count' in result['data'][0]:
                        result['value'] = result['data'][0]['count']
                    elif 'customer_count' in result['data'][0]:
                        result['value'] = result['data'][0]['customer_count']
            
            # If it's an error response, include the query
            if result.get('type') == 'error':
                if 'query' not in result:
                    result['query'] = request.query
                return result
                
            # Handle successful query response
            if 'value' in result:
                return {
                    "type": result['type'],
                    "label": result.get('label', 'Result'),
                    "value": result['value'],
                    "query": request.query,
                    "sql": result.get('sql', '')
                }
            elif 'rows' in result:
                return {
                    "type": "table",
                    "columns": result.get('columns', []),
                    "rows": result.get('rows', []),
                    "query": request.query,
                    "sql": result.get('sql', '')
                }
            elif 'data' in result:
                return {
                    "type": "table",
                    "columns": list(result.get('data', [{}])[0].keys()) if result.get('data') else [],
                    "rows": result.get('data', []),
                    "query": request.query,
                    "sql": result.get('sql', '')
                }
            return result
            
        # Handle list results (legacy format)
        if isinstance(result, list):
            if not result:  # Empty result
                return {
                    "type": "table",
                    "columns": [],
                    "rows": [],
                    "query": request.query,
                    "sql": request.query
                }
                
            # If it's a count query (single value)
            if len(result) == 1 and 'customer_count' in result[0]:
                return {
                    "type": "metric",
                    "label": "Total Customers",
                    "value": result[0]['customer_count'],
                    "query": request.query,
                    "sql": request.query
                }
                
            # Regular table result
            columns = list(result[0].keys()) if result else []
            return {
                "type": "table",
                "columns": columns,
                "rows": result,
                "query": request.query,
                "sql": request.query
            }
            
        # Fallback for any other result type
        return {
            "type": "error",
            "message": f"Unexpected result type: {type(result).__name__}",
            "query": request.query
        }
        
    except Exception as e:
        return {
            "type": "error",
            "message": str(e),
            "query": request.query
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
