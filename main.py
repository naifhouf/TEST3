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
# RECEIVE TICKS → BUILD CANDLES
# ======================
async def receive_price_data(data):
    try:
        symbol = data.get("symbol")
        price = data.get("price")

        if not symbol or price is None:
            return

        ts = now_minute_ts()

        # init candle
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

        # 🔥 candle closed → SEND
        payload = {
            "symbol": symbol,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "timestamp": candle["t0"],
            "is_closed": True
        }

        remove = []
        for ws, s in connections.items():
            try:
                if s == symbol:
                    await ws.send_json(payload)
            except:
                remove.append(ws)

        for ws in remove:
            connections.pop(ws, None)

        # start new candle
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
                logger.info("✅ Connected to PocketOption")
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

        logger.info(f"📡 Subscribed: {symbol}")

        asyncio.create_task(
            client._request_candles2(asset=symbol, timeframe=60)
        )

        while True:
