from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
import time
import logging
from pocketoptionapi_async import AsyncPocketOptionClient

logging.basicConfig(level=logging.INFO)
app = FastAPI()

SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'
client = AsyncPocketOptionClient(SSID, is_demo=True)

connections = {}

@app.on_event("startup")
async def startup():
    await client.connect()
    logging.info("CONNECTED TO PO")

@app.websocket("/ws/candles")
async def ws(ws: WebSocket):
    await ws.accept()
    try:
        symbol = await ws.receive_text()
        connections[ws] = symbol

        while True:
            candles = await client._request_candles2(
                asset=symbol,
                timeframe=60
            )

            if candles and len(candles) > 0:
                c = candles[-1]
                await ws.send_json({
                    "symbol": symbol,
                    "open": c["open"],
                    "high": c["high"],
                    "low": c["low"],
                    "close": c["close"],
                    "timestamp": c["time"],
                    "is_closed": True
                })

            await asyncio.sleep(60)

    except WebSocketDisconnect:
        connections.pop(ws, None)
