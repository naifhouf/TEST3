from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import time
import logging
from pocketoptionapi_async import AsyncPocketOptionClient

# ======================
# LOGGING
# ======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PO_CANDLES_SERVER")

app = FastAPI()

# ======================
# POCKET OPTION
# ======================
SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'

client = AsyncPocketOptionClient(
    SSID=SSID,
    is_demo=True,
    enable_logging=False
)

# ======================
# STATE
# ======================
connections = {}   # websocket -> symbol
candles = {}       # symbol -> current candle

# ======================
# HELPERS
# ======================
def now_minute_ts():
    return int(time.time() // 60 * 60)

def new_candle(price, ts):
    return {
        "t0": ts,
        "open": price,
        "high": price,
        "low": price,
        "close": price
    }

# ======================
# RECEIVE PRICE STREAM
# ======================
async def receive_price_data(data):
    try:
        symbol = data.get("symbol")
        price = data.get("price")

        if not symbol or price is None:
            return

        ts = now_minute_ts()

        if symbol not in candles:
            candles[symbol] = new_candle(price, ts)
            return

        candle = candles[symbol]

        # same candle
        if candle["t0"] == ts:
            candle["high"] = max(candle["high"], price)
            candle["low"] = min(candle["low"], price)
            candle["close"] = price
            return

        # candle closed → SEND
        payload = {
            "symbol": symbol,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "timestamp": candle["t0"],
            "is_closed": True
        }

        dead = []
        for ws, s in connections.items():
            try:
                if s == symbol:
                    await ws.send_json(payload)
            except:
                dead.append(ws)

        for ws in dead:
            connections.pop(ws, None)

        candles[symbol] = new_candle(price, ts)

    except Exception as e:
        logger.error(f"CANDLE ERROR: {e}")

# ======================
# STARTUP
# ======================
@app.on_event("startup")
async def startup():
    async def connect():
        while True:
            try:
                await client.connect()
                client.receive_price_data = receive_price_data
                logger.info("✅ Connected to PocketOption (price stream)")
                break
            except Exception as e:
                logger.error(f"Connect error: {e}")
                await asyncio.sleep(5)

    asyncio.create_task(connect())

# ======================
# WEBSOCKET
# ======================
@app.websocket("/ws/candles")
async def ws_candles(ws: WebSocket):
    await ws.accept()
    logger.info("🔌 WebSocket connected")

    try:
        symbol = await ws.receive_text()
        connections[ws] = symbol
        candles.pop(symbol, None)

        logger.info(f"📡 Subscribed: {symbol}")

        # 🔥 هذا مهم: طلب price stream الحقيقي
        asyncio.create_task(
            client.subscribe_price(symbol)
        )

        while True:
            symbol = await ws.receive_text()
            connections[ws] = symbol
            candles.pop(symbol, None)

            logger.info(f"🔄 Switched to: {symbol}")
            asyncio.create_task(
                client.subscribe_price(symbol)
            )

    except WebSocketDisconnect:
        logger.info("❌ WebSocket disconnected")

    finally:
        connections.pop(ws, None)
