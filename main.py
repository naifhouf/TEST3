from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
from pocketoptionapi_async import AsyncPocketOptionClient
import logging

# ======================
# LOGGING
# ======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# ======================
# POCKET OPTION CLIENT
# ======================
SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'
client = AsyncPocketOptionClient(SSID, is_demo=True, enable_logging=False)

# ======================
# CONNECTIONS
# ======================
connections = {}  # { websocket: asset }

# ======================
# RECEIVE CANDLES (OHLCV ONLY)
# ======================
async def receive_price_data(data):
    """
    يستقبل بيانات الشموع من Pocket Option
    ويرسل آخر شمعة (OHLCV) للواجهة
    """
    try:
        symbol = data.get("symbol")
        candles = data.get("candles")

        # نتأكد أن البيانات شموع فعلًا
        if not symbol or not candles or not isinstance(candles, list):
            return

        candle = candles[-1]  # آخر شمعة فقط

        payload = {
            "symbol": symbol,
            "open": candle.get("open"),
            "high": candle.get("high"),
            "low": candle.get("low"),
            "close": candle.get("close"),
            "volume": candle.get("volume", 0),
            "timestamp": candle.get("time")
        }

        logger.info(f"🕯️ CANDLE {symbol} → {payload}")

        dead = []
        for ws, current_asset in connections.items():
            try:
                if current_asset == symbol:
                    await ws.send_json(payload)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                dead.append(ws)

        for ws in dead:
            connections.pop(ws, None)

    except Exception as e:
        logger.error(f"receive_price_data error: {e}")

# ======================
# STARTUP
# ======================
@app.on_event("startup")
async def startup_event():
    async def reconnect():
        while True:
            try:
                await client.connect()
                client.receive_price_data = receive_price_data
                logger.info("✅ Connected to Pocket Option (CANDLES MODE)")
                break
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                await asyncio.sleep(5)

    asyncio.create_task(reconnect())

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
        logger.info(f"📡 Asset subscribed: {asset}")

        asyncio.create_task(
            client._request_candles2(
                asset=asset,
                timeframe=60  # M1
            )
        )

        while True:
            # تغيير الزوج
            asset = await websocket.receive_text()
            connections[websocket] = asset
            logger.info(f"🔄 Asset changed: {asset}")

            asyncio.create_task(
                client._request_candles2(
                    asset=asset,
                    timeframe=60
                )
            )

    except WebSocketDisconnect:
        logger.info("❌ WebSocket disconnected")

    except Exception as e:
        logger.error(f"WebSocket exception: {e}")
        try:
            await websocket.send_json({"error": str(e)})
        except:
            pass

    finally:
        connections.pop(websocket, None)
        logger.info("🧹 Connection closed")
