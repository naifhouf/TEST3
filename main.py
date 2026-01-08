from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
from pocketoptionapi_async import AsyncPocketOptionClient
import logging
import time

logging.basicConfig(level=logging.INFO)
app = FastAPI()

SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'
client = AsyncPocketOptionClient(SSID, is_demo=True)

connections = {}

# تخزين آخر شمعة لكل زوج
last_candle = {}

async def receive_price_data(data):
    symbol = data.get("symbol")
    candles = data.get("candles")

    if not symbol or not candles:
        return

    candle = candles[-1]
    last_candle[symbol] = candle

    # بث الشمعة لكل العملاء
    for ws, asset in list(connections.items()):
        if asset == symbol:
            try:
                await ws.send_json({
                    "symbol": symbol,
                    "open": candle["open"],
                    "high": candle["high"],
                    "low": candle["low"],
                    "close": candle["close"],
                    "time": candle["time"]
                })
            except:
                connections.pop(ws, None)

@app.on_event("startup")
async def startup():
    await client.connect()
    client.receive_price_data = receive_price_data

@app.websocket("/ws/candles")
async def ws_candles(ws: WebSocket):
    await ws.accept()
    try:
        symbol = await ws.receive_text()
        connections[ws] = symbol

        # طلب بث مستمر
        while True:
            await client._request_candles2(asset=symbol, timeframe=60)
            await asyncio.sleep(1)

    except WebSocketDisconnect:
        connections.pop(ws, None)
