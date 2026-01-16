import os
import json
import math
import newrelic.agent
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as aioredis
import asyncpg
from pydantic import BaseModel

# Initialize New Relic (Ensure you have newrelic.ini or env vars set)
try:
    newrelic.agent.initialize()
except:
    pass # Continue if local and no config

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

# Fix Render's Postgres URL compatibility
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
    # destination_lat/long would go here in full prod

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

# --- HELPER: Haversine Distance ---
def calculate_distance(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None: return 0.0
    R = 6371 
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
    
    # USING 'rides_v3' TO ENSURE FRESH CLEAN TABLE
    async with pg_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS rides_v3 (
                id SERIAL PRIMARY KEY,
                rider_id INT,
                driver_id INT,
                pickup_lat DOUBLE PRECISION,
                pickup_long DOUBLE PRECISION,
                status VARCHAR(20), -- REQUESTED, MATCHED, ACCEPTED, IN_PROGRESS, PAUSED, COMPLETED, PAID
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

# --- API 1: DRIVER LOCATION (High Velocity) ---
@app.post("/v1/drivers/{driver_id}/location")
async def update_location(driver_id: int, loc: LocationUpdate):
    # Optimally, this only hits Redis, not Postgres, to handle 200k/sec
    await redis.geoadd("drivers", [loc.long, loc.lat, str(driver_id)])
    # Emit for UI demo only
    await manager.broadcast({"type": "DRIVER_MOVED", "id": driver_id, "lat": loc.lat, "long": loc.long})
    return {"status": "ok"}

# --- API 2: CREATE RIDE REQUEST ---
@app.post("/v1/rides")
async def create_ride(ride: RideRequest):
    # 1. Match Driver (Fast P95 < 1s)
    nearest = await redis.geosearch(
        name="drivers", longitude=ride.pickup_long, latitude=ride.pickup_lat, radius=5000, unit="km", count=1, sort="ASC"
    )
    if not nearest:
        # Edge Case: Retry logic would go here in prod, or queueing
        raise HTTPException(status_code=404, detail="No drivers found nearby")

    driver_id = int(nearest[0])
    
    # 2. Create Transaction
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO rides_v3 (rider_id, driver_id, pickup_lat, pickup_long, status, fare) 
            VALUES ($1, $2, $3, $4, 'REQUESTED', 0) 
            RETURNING id
            """,
            ride.rider_id, driver_id, ride.pickup_lat, ride.pickup_long
        )

    # 3. Notify
    msg = {
        "type": "RIDE_UPDATE", "ride_id": row['id'], "status": "REQUESTED", 
        "driver_id": driver_id, "detail": f"Driver {driver_id} found. Waiting for acceptance..."
    }
    await manager.broadcast(msg)
    return msg

# --- API 3: GET RIDE STATUS (Requirement Check) ---
@app.get("/v1/rides/{id}")
async def get_ride(id: int):
    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM rides_v3 WHERE id=$1", id)
        if not row: raise HTTPException(status_code=404, detail="Ride not found")
        return dict(row)

# --- API 4: DRIVER ACCEPTS RIDE ---
@app.post("/v1/drivers/{id}/accept")
async def accept_ride(id: int, action: TripAction):
    async with pg_pool.acquire() as conn:
        # Verify ride is still in REQUESTED state (Race condition handling)
        result = await conn.execute(
            "UPDATE rides_v3 SET status='ACCEPTED' WHERE id=$1 AND status='REQUESTED'", 
            action.ride_id
        )
        if "0" in result: # No rows updated
            raise HTTPException(status_code=409, detail="Ride already taken or cancelled")

    msg = {"type": "RIDE_UPDATE", "ride_id": action.ride_id, "status": "ACCEPTED", "detail": "Driver Accepted!"}
    await manager.broadcast(msg)
    return {"status": "accepted"}

# --- API 5: START TRIP ---
@app.post("/v1/trips/{id}/start")
async def start_trip(id: int):
    async with pg_pool.acquire() as conn:
        # Idempotency: Don't restart if already in progress
        await conn.execute("UPDATE rides_v3 SET status='IN_PROGRESS', start_time=NOW() WHERE id=$1 AND status='ACCEPTED'", id)
    
    msg = {"type": "RIDE_UPDATE", "ride_id": id, "status": "IN_PROGRESS", "detail": "Trip Started"}
    await manager.broadcast(msg)
    return {"status": "started"}

# --- API 6: PAUSE TRIP (Requirement Check) ---
@app.post("/v1/trips/{id}/pause")
async def pause_trip(id: int):
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE rides_v3 SET status='PAUSED' WHERE id=$1", id)
    
    msg = {"type": "RIDE_UPDATE", "ride_id": id, "status": "PAUSED", "detail": "Trip Paused"}
    await manager.broadcast(msg)
    return {"status": "paused"}

# --- API 7: END TRIP (Calculates Fare) ---
@app.post("/v1/trips/{id}/end")
async def end_trip(id: int):
    drop_lat, drop_long = 13.0, 77.6 # Mock destination
    
    async with pg_pool.acquire() as conn:
        # Idempotency Check
        ride = await conn.fetchrow("SELECT pickup_lat, pickup_long, status, fare FROM rides_v3 WHERE id=$1", id)
        if not ride: raise HTTPException(status_code=404)
        
        # If already completed, just return the existing fare (Idempotency)
        if ride['status'] == 'COMPLETED':
            return {"status": "ended", "fare": ride['fare']}

        dist_km = calculate_distance(ride['pickup_lat'], ride['pickup_long'], drop_lat, drop_long)
        fare = round(50 + (dist_km * 12), 2)

        await conn.execute(
            "UPDATE rides_v3 SET status='COMPLETED', end_time=NOW(), fare=$2 WHERE id=$1", 
            id, fare
        )
    
    msg = {"type": "RIDE_UPDATE", "ride_id": id, "status": "COMPLETED", "fare": fare, "detail": f"Trip Ended. Fare: ${fare}"}
    await manager.broadcast(msg)
    return {"status": "ended", "fare": fare}

# --- API 8: PAYMENTS ---
@app.post("/v1/payments")
async def process_payment(action: TripAction):
    # External PSP Mock
    import random
    success = random.choice([True, True, True, False]) 
    status = "PAID" if success else "FAILED"
    
    async with pg_pool.acquire() as conn:
        await conn.execute("UPDATE rides_v3 SET payment_status=$2 WHERE id=$1", action.ride_id, status)

    msg = {"type": "RIDE_UPDATE", "ride_id": action.ride_id, "status": status, "detail": f"Payment {status}"}
    await manager.broadcast(msg)
    return {"payment_status": status}

# --- FRONTEND SERVING ---
@app.get("/")
async def get():
    return HTMLResponse(content=html_content)

# HTML Content separated for cleanliness (See next block)
html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GoComet DAW - Final</title>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <style>
        body { font-family: 'Segoe UI', sans-serif; display: flex; height: 100vh; margin: 0; }
        #map { flex: 2; }
        #sidebar { flex: 1; padding: 20px; background: #f8f9fa; display: flex; flex-direction: column; gap: 10px; overflow-y: auto; border-left: 2px solid #ddd; }
        .card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        button { width: 100%; padding: 8px; border: none; border-radius: 5px; cursor: pointer; color: white; font-weight: bold; margin-top: 5px; }
        .btn-req { background: #007bff; } 
        .btn-acc { background: #17a2b8; }
        .btn-start { background: #28a745; } 
        .btn-pause { background: #6c757d; }
        .btn-end { background: #dc3545; } 
        .btn-pay { background: #ffc107; color: black; }
        .log { font-size: 12px; margin-bottom: 5px; padding: 5px; border-bottom: 1px solid #eee; }
        h2, h3 { margin: 0 0 10px 0; }
    </style>
</head>
<body>
    <div id="map"></div>
    <div id="sidebar">
        <div class="card">
            <h3>1. Rider</h3>
            <button class="btn-req" onclick="requestRide()">Request Ride</button>
            <div id="rider-status" style="font-size:12px; margin-top:5px;">Idle</div>
        </div>

        <div class="card">
            <h3>2. Driver Actions</h3>
            <button class="btn-acc" onclick="acceptRide()">Accept Ride</button>
            <button class="btn-start" onclick="startTrip()">Start Trip</button>
            <button class="btn-pause" onclick="pauseTrip()">Pause Trip</button>
            <button class="btn-end" onclick="endTrip()">End Trip</button>
        </div>

        <div class="card">
            <h3>3. Finance</h3>
            <button class="btn-pay" onclick="pay()">Process Payment</button>
        </div>

        <div class="card" style="flex: 1;">
            <h3>Logs</h3>
            <div id="logs"></div>
        </div>
    </div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script>
        const map = L.map('map').setView([12.9716, 77.5946], 13);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);

        let currentRideId = null;
        let currentDriverId = null;
        
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            if (data.type === 'RIDE_UPDATE') {
                log(`[${data.status}] ${data.detail}`);
                document.getElementById("rider-status").innerText = "Status: " + data.status;
                
                if(data.ride_id) currentRideId = data.ride_id;
                if(data.driver_id) currentDriverId = data.driver_id;
                if(data.fare) alert("Trip Done! Fare: $" + data.fare);
            }
            
            if (data.type === 'DRIVER_MOVED') {
                log(`Driver ${data.id} moved`);
                L.marker([data.lat, data.long]).addTo(map).bindPopup("Driver " + data.id).openPopup();
            }
        };

        // --- ACTIONS ---
        async function requestRide() {
            try {
                // Seed driver to prevent 404
                await fetch("/v1/drivers/101/location", {
                    method: "POST", headers: {"Content-Type": "application/json"},
                    body: JSON.stringify({ lat: 12.9716, long: 77.5946 }) 
                });
                
                await fetch("/v1/rides", {
                    method: "POST", headers: {"Content-Type": "application/json"},
                    body: JSON.stringify({ rider_id: 99, pickup_lat: 12.9716, pickup_long: 77.5946 })
                });
            } catch(e) { alert(e); }
        }

        async function acceptRide() {
            if(!currentRideId || !currentDriverId) return alert("No requested ride found!");
            await fetch(`/v1/drivers/${currentDriverId}/accept`, {
                method: "POST", headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ ride_id: currentRideId })
            });
        }

        async function startTrip() {
            if(!currentRideId) return alert("No Active Ride!");
            await fetch(`/v1/trips/${currentRideId}/start`, { method: "POST" });
        }

        async function pauseTrip() {
            if(!currentRideId) return alert("No Active Ride!");
            await fetch(`/v1/trips/${currentRideId}/pause`, { method: "POST" });
        }

        async function endTrip() {
            if(!currentRideId) return alert("No Active Ride!");
            await fetch(`/v1/trips/${currentRideId}/end`, { method: "POST" });
        }

        async function pay() {
            if(!currentRideId) return alert("No Active Ride!");
            await fetch(`/v1/payments`, {
                method: "POST", headers: {"Content-Type": "application/json"},
                body: JSON.stringify({ ride_id: currentRideId })
            });
        }

        function log(msg) {
            const d = document.getElementById("logs");
            d.innerHTML = `<div class="log">${msg}</div>` + d.innerHTML;
        }
    </script>
</body>
</html>
"""

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
