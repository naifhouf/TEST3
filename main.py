from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import logging
import time
from pocketoptionapi_async import AsyncPocketOptionClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PO_CANDLES")

app = FastAPI()

SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'

client = AsyncPocketOptionClient(
    SSID=SSID,
    is_demo=True,
    enable_logging=False
)

connections = {}  # { websocket: asset }
last_candles = {}  # { symbol: current_candle }

TIMEFRAME = 60  # ثانية

# ======================
# RECEIVE DATA
# ======================
async def receive_price_data(data):
    try:
        symbol = data.get("symbol")
        price = data.get("price")
        ts = int(time.time())

        if not symbol or price is None:
            return

        candle_start = ts - (ts % TIMEFRAME)

        # إنشاء شمعة جديدة
        if symbol not in last_candles or last_candles[symbol]["t"] != candle_start:
            last_candles[symbol] = {
                "t": candle_start,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1
            }
        else:
            c = last_candles[symbol]
            c["high"] = max(c["high"], price)
            c["low"] = min(c["low"], price)
            c["close"] = price
            c["volume"] += 1

        payload = {
            "symbol": symbol,
            "open": last_candles[symbol]["open"],
            "high": last_candles[symbol]["high"],
            "low": last_candles[symbol]["low"],
            "close": last_candles[symbol]["close"],
            "volume": last_candles[symbol]["volume"],
            "timestamp": last_candles[symbol]["t"]
        }

        remove = []
        for ws, asset in connections.items():
            if asset == symbol:
                try:
                    await ws.send_json(payload)
                except:
                    remove.append(ws)

        for ws in remove:
            connections.pop(ws, None)

    except Exception as e:
        logger.error(f"Receive error: {e}")

# ======================
# STARTUP
# ======================
@app.on_event("startup")
async def startup_event():
    async def connect():
        while True:
            try:
                await client.connect()
                client.receive_price_data = receive_price_data
                logger.info("✅ Connected to PocketOption (CANDLES MODE)")
                break
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                await asyncio.sleep(5)

    asyncio.create_task(connect())

# ======================
# WEBSOCKET
# ======================
@app.websocket("/ws/candles")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("🔌 WebSocket connected")

    try:
        asset = await websocket.receive_text()
        connections[websocket] = asset

        logger.info(f"📡 Subscribed: {asset}")

        # فقط اشتراك – لا نرسل تيكات
        asyncio.create_task(
            client._request_candles2(asset=asset, timeframe=TIMEFRAME)
        )

        while True:
            asset = await websocket.receive_text()
            connections[websocket] = asset
            logger.info(f"🔄 Asset changed: {asset}")

            asyncio.create_task(
                client._request_candles2(asset=asset, timeframe=TIMEFRAME)
            )

    except WebSocketDisconnect:
        logger.info("❌ WebSocket disconnected")

    finally:
        connections.pop(websocket, None)
