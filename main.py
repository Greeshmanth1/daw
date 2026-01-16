import os
import json
import math
from datetime import datetime
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

# --- CONFIGURATION ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/postgres")

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
    # In real app: drop_lat, drop_long, tier

class TripAction(BaseModel):
    ride_id: int

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

# --- HELPER: Haversine Distance (Lat/Long to KM) ---
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# --- LIFECYCLE ---
@app.on_event("startup")
async def startup():
    global redis, pg_pool
    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    pg_pool = await asyncpg.create_pool(DATABASE_URL)
    
    # Ensure Table Exists with ALL columns
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
                created_at TIMESTAMP DEFAULT NOW(),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                payment_status VARCHAR(20) DEFAULT 'PENDING'
            );
        """)

@app.on_event("shutdown")
async def shutdown():
    if redis: await redis.close()
    if pg_pool: await pg_pool.close()

# --- API 1: REQUEST RIDE (Matching) ---
@app.post("/v1/rides")
async def create_ride(ride: RideRequest):
    nearest = await redis.geosearch(
        name="drivers", longitude=ride.pickup_long, latitude=ride.pickup_lat, radius=5000, unit="km", count=1, sort="ASC"
    )
    if not nearest:
        raise HTTPException(status_code=404, detail="No drivers found nearby")

    driver_id = int(nearest[0])
    
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO rides (rider_id, driver_id, status, fare) VALUES ($1, $2, 'ACCEPTED', 0) RETURNING id",
            ride.rider_id, driver_id
        )

    msg = {"type": "RIDE_UPDATE", "ride_id": row['id'], "status": "ACCEPTED", "driver_id": driver_id, "detail": "Driver is on the way"}
    await manager.broadcast(msg)
    return msg

# --- API 2: START TRIP ---
@app.post("/v1/trips/{id}/start")
async def start_trip(id: int):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE rides SET status='IN_PROGRESS', start_time=NOW() WHERE id=$1", id)
    
    msg = {"type": "RIDE_UPDATE", "ride_id": id, "status": "IN_PROGRESS", "detail": "Ride Started! Enjoy your trip."}
    await manager.broadcast(msg)
    return {"status": "started"}

# --- API 3: END TRIP (Calc Fare) ---
@app.post("/v1/trips/{id}/end")
async def end_trip(id: int):
    # Mocking a drop location (10km away) for fare calc
    drop_lat, drop_long = 13.0, 77.6 
    
    async with pg_pool.acquire() as conn:
        # Fetch pickup location to calc distance
        ride = await conn.fetchrow("SELECT pickup_lat, pickup_long FROM rides WHERE id=$1", id)
        
        dist_km = calculate_distance(ride['pickup_lat'], ride['pickup_long'], drop_lat, drop_long)
        fare = round(50 + (dist_km * 12), 2) # Base 50 + 12 per km

        await conn.execute(
            "UPDATE rides SET status='COMPLETED', end_time=NOW(), fare=$2 WHERE id=$1", 
            id, fare
        )
    
    msg = {"type": "RIDE_UPDATE", "ride_id": id, "status": "COMPLETED", "fare": fare, "detail": f"Trip Ended. Fare: ${fare}"}
    await manager.broadcast(msg)
    return {"status": "ended", "fare": fare}

# --- API 4: PAYMENTS ---
@app.post("/v1/payments")
async def process_payment(action: TripAction):
    # Mock External PSP Call
    import random
    success = random.choice([True, True, True, False]) # 75% success rate
    
    status = "PAID" if success else "FAILED"
    
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE rides SET payment_status=$2 WHERE id=$1", action.ride_id, status)

    msg = {"type": "RIDE_UPDATE", "ride_id": action.ride_id, "status": status, "detail": f"Payment {status}"}
    await manager.broadcast(msg)
    return {"payment_status": status}

# --- API 5: DRIVER LOCATION ---
@app.post("/v1/drivers/{driver_id}/location")
async def update_location(driver_id: int, loc: LocationUpdate):
    await redis.geoadd("drivers", [loc.long, loc.lat, str(driver_id)])
    await manager.broadcast({"type": "DRIVER_MOVED", "id": driver_id, "lat": loc.lat, "long": loc.long})
    return {"status": "ok"}

# --- FRONTEND ---
@app.get("/")
async def get():
    return HTMLResponse(content=f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>GoComet DAW - Complete System</title>
        <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; display: flex; height: 100vh; margin: 0; }}
            #map {{ flex: 2; }}
            #sidebar {{ flex: 1; padding: 20px; background: #f8f9fa; display: flex; flex-direction: column; gap: 15px; overflow-y: auto; border-left: 2px solid #ddd; }}
            .card {{ background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            button {{ width: 100%; padding: 10px; border: none; border-radius: 5px; cursor: pointer; color: white; font-weight: bold; margin-top: 5px; }}
            .btn-req {{ background: #007bff; }} .btn-start {{ background: #28a745; }} .btn-end {{ background: #dc3545; }} .btn-pay {{ background: #ffc107; color: black; }}
            .log {{ font-size: 12px; margin-bottom: 5px; padding: 5px; border-bottom: 1px solid #eee; }}
            h2, h3 {{ margin: 0 0 10px 0; }}
        </style>
    </head>
    <body>
        <div id="map"></div>
        <div id="sidebar">
            <div class="card">
                <h2>Rider App</h2>
                <button class="btn-req" onclick="requestRide()">1. Request Ride</button>
                <div id="rider-status" style="margin-top:10px; color: #666;">Status: Idle</div>
            </div>

            <div class="card">
                <h2>Driver App (Simulation)</h2>
                <button class="btn-start" onclick="startTrip()">2. Start Trip</button>
                <button class="btn-end" onclick="endTrip()">3. End Trip</button>
            </div>

            <div class="card">
                <h2>Payments</h2>
                <button class="btn-pay" onclick="pay()">4. Process Payment</button>
            </div>

            <div class="card" style="flex: 1;">
                <h3>Live Logs</h3>
                <div id="logs"></div>
            </div>
        </div>

        <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
        <script>
            const map = L.map('map').setView([12.9716, 77.5946], 13);
            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png').addTo(map);

            let currentRideId = null;
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const ws = new WebSocket(`${{protocol}}//${{window.location.host}}/ws`);

            // --- WEBSOCKET HANDLER ---
            ws.onmessage = (event) => {{
                const data = JSON.parse(event.data);
                log(data.type === 'RIDE_UPDATE' ? data.detail : `Driver ${{data.id}} Moved`);

                if (data.type === 'DRIVER_MOVED') {{
                    L.marker([data.lat, data.long]).addTo(map).bindPopup("Driver").openPopup();
                }}
                
                if (data.type === 'RIDE_UPDATE') {{
                    document.getElementById("rider-status").innerText = "Status: " + data.status;
                    if(data.ride_id) currentRideId = data.ride_id;
                    if(data.fare) alert("Trip Done! Fare: $" + data.fare);
                }}
            }};

            // --- API CALLS ---
            async function requestRide() {{
                try {{
                    // Add a mock driver first to ensure match
                    await fetch("/v1/drivers/101/location", {{
                        method: "POST", headers: {{"Content-Type": "application/json"}},
                        body: JSON.stringify({{ lat: 12.9716, long: 77.5946 }}) 
                    }});
                    
                    await fetch("/v1/rides", {{
                        method: "POST", headers: {{"Content-Type": "application/json"}},
                        body: JSON.stringify({{ rider_id: 99, pickup_lat: 12.9716, pickup_long: 77.5946 }})
                    }});
                }} catch(e) {{ alert(e); }}
            }}

            async function startTrip() {{
                if(!currentRideId) return alert("No Active Ride!");
                await fetch(`/v1/trips/${{currentRideId}}/start`, {{ method: "POST" }});
            }}

            async function endTrip() {{
                if(!currentRideId) return alert("No Active Ride!");
                await fetch(`/v1/trips/${{currentRideId}}/end`, {{ method: "POST" }});
            }}

            async function pay() {{
                if(!currentRideId) return alert("No Active Ride!");
                await fetch(`/v1/payments`, {{
                    method: "POST", headers: {{"Content-Type": "application/json"}},
                    body: JSON.stringify({{ ride_id: currentRideId }})
                }});
            }}

            function log(msg) {{
                const d = document.getElementById("logs");
                d.innerHTML = `<div class="log">${{msg}}</div>` + d.innerHTML;
            }}
        </script>
    </body>
    </html>
    """)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
