import os
import json
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as aioredis
import asyncpg
from pydantic import BaseModel

app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CONFIGURATION (Reads from Cloud Env or defaults to Local) ---
# Render provides internal URLs starting with rediss:// or postgres://
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/postgres")

# Handle Render's specific Postgres requirement (replace postgres:// with postgresql://)
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# --- POOLS ---
redis = None
pg_pool = None

# --- MODELS ---
class LocationUpdate(BaseModel):
    lat: float
    long: float

class RideRequest(BaseModel):
    rider_id: int
    pickup_lat: float
    pickup_long: float

# --- WEBSOCKET MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# --- LIFECYCLE ---
@app.on_event("startup")
async def startup():
    global redis, pg_pool
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    pg_pool = await asyncpg.create_pool(DATABASE_URL)
    
    # Create Table Automatically on Startup (Survival Mode!)
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rides (
                id SERIAL PRIMARY KEY,
                rider_id INT,
                driver_id INT,
                pickup_lat DOUBLE PRECISION,
                pickup_long DOUBLE PRECISION,
                status VARCHAR(20),
                fare DECIMAL(10, 2),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)

@app.on_event("shutdown")
async def shutdown():
    if redis: await redis.close()
    if pg_pool: await pg_pool.close()

# --- FRONTEND SERVING (The Magic Trick) ---
@app.get("/")
async def get():
    # Helper to determine WebSocket URL based on environment (Secure vs Insecure)
    # When deployed, Render uses HTTPS, so we need WSS
    return HTMLResponse(content=f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>GoComet DAW - Live Ops</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <style>
            body {{ font-family: sans-serif; display: flex; height: 100vh; margin: 0; }}
            #map {{ flex: 2; }}
            #sidebar {{ flex: 1; padding: 20px; background: #f4f4f4; overflow-y: auto; }}
            .log {{ background: white; padding: 10px; margin-bottom: 5px; border-left: 4px solid #007bff; }}
        </style>
    </head>
    <body>
        <div id="map"></div>
        <div id="sidebar">
            <h2>Operations Dashboard</h2>
            <button onclick="requestRide()" style="padding:10px 20px; font-size:16px; cursor:pointer; width: 100%;">
                Request Ride (Simulate)
            </button>
            <div id="status" style="margin-top:20px; font-weight: bold;">Waiting...</div>
            <div id="logs" style="margin-top:20px;"></div>
        </div>
        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script>
            const map = L.map('map').setView([12.9716, 77.5946], 13);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png').addTo(map);

            const drivers = {{}};
            
            // Auto-detect WebSocket protocol (ws:// or wss://)
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${{protocol}}//${{window.location.host}}/ws`;
            const ws = new WebSocket(wsUrl);

            ws.onmessage = (event) => {{
                const data = JSON.parse(event.data);
                const logDiv = document.getElementById("logs");
                
                if (data.type === "DRIVER_MOVED") {{
                    if (drivers[data.id]) {{
                        drivers[data.id].setLatLng([data.lat, data.long]);
                    }} else {{
                        drivers[data.id] = L.marker([data.lat, data.long]).addTo(map)
                            .bindPopup("Driver " + data.id).openPopup();
                    }}
                }} 
                else if (data.type === "RIDE_MATCHED") {{
                    const msg = `Ride #${{data.ride_id}} matched with Driver ${{data.driver_id}}`;
                    document.getElementById("status").innerText = msg;
                    logDiv.innerHTML = `<div class="log">${{msg}}</div>` + logDiv.innerHTML;
                }}
            }};

            async function requestRide() {{
                await fetch("/v1/rides", {{
                    method: "POST",
                    headers: {{ "Content-Type": "application/json" }},
                    body: JSON.stringify({{
                        rider_id: Math.floor(Math.random() * 1000),
                        pickup_lat: 12.9716, 
                        pickup_long: 77.5946 
                    }})
                }});
            }}
        </script>
    </body>
    </html>
    """)

# --- APIs (Same as before) ---
@app.post("/v1/drivers/{driver_id}/location")
async def update_location(driver_id: int, loc: LocationUpdate):
    await redis.geoadd("drivers", [loc.long, loc.lat, str(driver_id)])
    await manager.broadcast({"type": "DRIVER_MOVED", "id": driver_id, "lat": loc.lat, "long": loc.long})
    return {"status": "ok"}

@app.post("/v1/rides")
async def create_ride(ride: RideRequest):
    nearest_drivers = await redis.geosearch(
        name="drivers", longitude=ride.pickup_long, latitude=ride.pickup_lat, radius=5000, unit="km", count=1, sort="ASC"
    )
    if not nearest_drivers:
        raise HTTPException(status_code=404, detail="No drivers found nearby")

    driver_id = int(nearest_drivers[0])
    
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO rides (rider_id, driver_id, status, fare) VALUES ($1, $2, 'ACCEPTED', 150.00) RETURNING id",
            ride.rider_id, driver_id
        )

    ride_info = {"type": "RIDE_MATCHED", "ride_id": row['id'], "driver_id": driver_id, "status": "ACCEPTED"}
    await manager.broadcast(ride_info)
    return ride_info

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
