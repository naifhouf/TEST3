from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import asyncio
from pocketoptionapi_async import AsyncPocketOptionClient
import logging

# إعداد التسجيل لتتبع الأحداث والأخطاء
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# SSID من الكود الأصلي
SSID = r'42["auth",{"session":"c6v74skiu8l58ls0k2iesll1fa","isDemo":1,"uid":71923919,"platform":2,"isFastHistory":true}]'
client = AsyncPocketOptionClient(SSID, is_demo=True, enable_logging=False)

# قاموس لتتبع الزوج الحالي لكل اتصال WebSocket
connections = {}  # {websocket: current_asset}

async def receive_price_data(data):
    """إرسال بيانات الأسعار للعملاء بناءً على الزوج الحالي"""
    logger.info(f"البيانات الخام: {data}")
    to_remove = []
    # استخراج الزوج من مفتاح 'symbol'
    received_symbol = data.get("symbol", None)
    if received_symbol is None:
        logger.warning("البيانات مافيهاش مفتاح 'symbol'، تجاهل البيانات")
        return
    for ws, current_asset in connections.items():
        try:
            # إرسال البيانات فقط إذا كانت للزوج الحالي
            if received_symbol == current_asset:
                price = data.get("price", 0.0)
                price_data = {"price": price, "symbol": received_symbol}  # إضافة 'symbol' للواجهة الأمامية
                await ws.send_json(price_data)
                logger.info(f"تم إرسال البيانات للزوج {current_asset}: {price_data}")
            else:
                logger.debug(f"تجاهل البيانات للزوج {received_symbol}، الزوج الحالي: {current_asset}")
        except Exception as e:
            logger.error(f"خطأ في WebSocket: {e}")
            to_remove.append(ws)
    for ws in to_remove:
        connections.pop(ws, None)

@app.on_event("startup")
async def startup_event():
    """الاتصال بـ AsyncPocketOptionClient عند بدء التطبيق"""
    async def reconnect():
        while True:
            try:
                await client.connect()
                logger.info("تم الاتصال بـ PocketOption")
                client.receive_price_data = receive_price_data
                break
            except Exception as e:
                logger.error(f"فشل الاتصال بـ PocketOption: {e}")
                await asyncio.sleep(5)  # إعادة المحاولة بعد 5 ثواني
    asyncio.create_task(reconnect())

@app.websocket("/ws/prices")
async def websocket_endpoint(websocket: WebSocket):
    """التعامل مع اتصالات WebSocket من الواجهة الأمامية"""
    await websocket.accept()
    logger.info("اتصال WebSocket جديد")

    try:
        # أول رسالة هي اسم الزوج (مثل EURUSD أو GBPUSD_otc)
        msg = await websocket.receive_text()
        logger.info(f"العميل طلب الزوج: {msg}")
        connections[websocket] = msg
        asyncio.create_task(client._request_candles2(asset=msg, timeframe=60))

        # الاستمرار في استقبال الرسائل لتحديث الزوج
        while True:
            msg = await websocket.receive_text()
            logger.info(f"العميل طلب تحديث الزوج: {msg}")
            connections[websocket] = msg
            asyncio.create_task(client._request_candles2(asset=msg, timeframe=60))

    except WebSocketDisconnect:
        logger.info("العميل قطع الاتصال")
    except Exception as e:
        logger.error(f"خطأ في WebSocket: {e}")
        try:
            await websocket.send_json({"error": str(e)})
        except:
            pass
    finally:
        connections.pop(websocket, None)
        logger.info("تم إغلاق اتصال WebSocket")
