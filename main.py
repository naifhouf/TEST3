from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
from pocketoptionapi_async import AsyncPocketOptionClient
import logging
from collections import defaultdict, deque
import time

# ======================
# LOGGING
# ======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PO_CANDLES")

# ======================
# FASTAPI APP
# ======================
app = FastAPI()

# ======================
# POCKET OPTION CLIENT
# ======================
SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'

client = AsyncPocketOptionClient(
    SSID=SSID,
    is_demo=True,
    enable_logging=False
)

# ======================
# CONNECTIONS & STORAGE
# ======================
connections = {}  # { websocket: asset }
CANDLE_STORE = defaultdict(lambda: deque(maxlen=500))  # تخزين آخر 500 شمعة لكل زوج

# ======================
# RECEIVE CANDLES (OHLC)
# ======================
async def receive_price_data(data):
    try:
        symbol = data.get("symbol")
        candles = data.get("candles")

        if not symbol or not candles:
            return

        candle = candles[-1]

        # التأكد أن الشمعة مغلقة فعليًا (مزامنة :00)
        now_ms = int(time.time() * 1000)
        candle_time_ms = candle.get("time", 0) * 1000

        if candle_time_ms > now_ms - 1000:
            return  # الشمعة لم تُغلق بعد

        payload = {
            "symbol": symbol,
            "open": candle.get("open"),
            "high": candle.get("high"),
            "low": candle.get("low"),
            "close": candle.get("close"),
            "volume": candle.get("volume"),
            "timestamp": candle.get("time")
        }

        # تخزين الشمعة في السيرفر
        CANDLE_STORE[symbol].append(payload)

        logger.info(f"📊 CANDLE CLOSED {symbol} → {payload}")

        # إرسال الشمعة لكل العملاء المهتمين بهذا الزوج
        remove = []
        for ws, asset in connections.items():
            try:
                if asset == symbol:
                    await ws.send_json(payload)
            except Exception as e:
                logger.error(f"WebSocket send error: {e}")
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
                logger.info("✅ Connected to PocketOption")
                break
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                await asyncio.sleep(5)

    asyncio.create_task(connect())

# ======================
# WEBSOCKET ENDPOINT
# ======================
@app.websocket("/ws/candles")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("🔌 WebSocket connected")

    try:
        # أول رسالة = اسم الزوج
        asset = await websocket.receive_text()
        connections[websocket] = asset
        logger.info(f"📡 Asset requested: {asset}")

        # إرسال الشموع المخزنة فورًا (History)
        for candle in CANDLE_STORE.get(asset, []):
            await websocket.send_json(candle)

        # طلب شموع جديدة من Pocket Option
        asyncio.create_task(
            client._request_candles2(
                asset=asset,
                timeframe=60  # M1
            )
        )

        while True:
            # تغيير الزوج عند الطلب
            asset = await websocket.receive_text()
            connections[websocket] = asset
            logger.info(f"🔄 Asset changed to: {asset}")

            # إرسال التاريخ المخزن
            for candle in CANDLE_STORE.get(asset, []):
                await websocket.send_json(candle)

            asyncio.create_task(
                client._request_candles2(
                    asset=asset,
                    timeframe=60
                )
            )

    except WebSocketDisconnect:
        logger.info("❌ WebSocket disconnected")

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.send_json({"error": str(e)})
        except:
            pass

    finally:
        connections.pop(websocket, None)
        logger.info("🧹 Connection closed")
