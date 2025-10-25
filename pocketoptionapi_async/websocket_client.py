"""
Async WebSocket client for PocketOption API
"""

import asyncio
import json
import ssl
import time
from typing import Optional, Callable, Dict, Any, List, Deque
from datetime import datetime
from collections import deque
import websockets
from websockets.exceptions import ConnectionClosed
from websockets.legacy.client import WebSocketClientProtocol
from loguru import logger

from .models import ConnectionInfo, ConnectionStatus, ServerTime
from .constants import CONNECTION_SETTINGS, DEFAULT_HEADERS
from .exceptions import WebSocketError, ConnectionError


class MessageBatcher:
    """Batch messages to improve performance"""

    def __init__(self, batch_size: int = 10, batch_timeout: float = 0.1):
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.pending_messages: Deque[str] = deque()
        self._last_batch_time = time.time()
        self._batch_lock = asyncio.Lock()

    async def add_message(self, message: str) -> List[str]:
        """Add message to batch and return batch if ready"""
        async with self._batch_lock:
            self.pending_messages.append(message)
            current_time = time.time()

            # Check if batch is ready
            if (
                len(self.pending_messages) >= self.batch_size
                or current_time - self._last_batch_time >= self.batch_timeout
            ):
                batch = list(self.pending_messages)
                self.pending_messages.clear()
                self._last_batch_time = current_time
                return batch

            return []

    async def flush_batch(self) -> List[str]:
        """Force flush current batch"""
        async with self._batch_lock:
            if self.pending_messages:
                batch = list(self.pending_messages)
                self.pending_messages.clear()
                self._last_batch_time = time.time()
                return batch
            return []


class ConnectionPool:
    """Connection pool for better resource management"""

    def __init__(self, max_connections: int = 3):
        self.active_connections: Dict[str, WebSocketClientProtocol] = {}
        self.connection_stats: Dict[str, Dict[str, Any]] = {}
        self._pool_lock = asyncio.Lock()

    async def get_best_connection(self) -> Optional[str]:
        """Get the best performing connection URL"""
        async with self._pool_lock:
            if not self.connection_stats:
                return None

            # Sort by response time and success rate
            best_url = min(
                self.connection_stats.keys(),
                key=lambda url: (
                    self.connection_stats[url].get("avg_response_time", float("inf")),
                    -self.connection_stats[url].get("success_rate", 0),
                ),
            )
            return best_url

    async def update_stats(self, url: str, response_time: float, success: bool):
        """Update connection statistics"""
        async with self._pool_lock:
            if url not in self.connection_stats:
                self.connection_stats[url] = {
                    "response_times": deque(maxlen=100),
                    "successes": 0,
                    "failures": 0,
                    "avg_response_time": 0,
                    "success_rate": 0,
                }

            stats = self.connection_stats[url]
            stats["response_times"].append(response_time)

            if success:
                stats["successes"] += 1
            else:
                stats["failures"] += 1

            # Update averages
            if stats["response_times"]:
                stats["avg_response_time"] = sum(stats["response_times"]) / len(
                    stats["response_times"]
                )

            total_attempts = stats["successes"] + stats["failures"]
            if total_attempts > 0:
                stats["success_rate"] = stats["successes"] / total_attempts


class AsyncWebSocketClient:
    """
    Professional async WebSocket client for PocketOption
    """

    def __init__(self):
        self.websocket: Optional[WebSocketClientProtocol] = None
        self.connection_info: Optional[ConnectionInfo] = None
        self.server_time: Optional[ServerTime] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._message_queue: asyncio.Queue = asyncio.Queue()
        self._event_handlers: Dict[str, List[Callable]] = {}
        self._parent_client = None  # هنربطها يدويًا من برّه
        self._parent_client2 = None  # هنربطها يدويًا من برّه
        self._parent_client3 = None  # هنربطها يدويًا من برّه
        self._deals_cache: Dict[str, Dict[str, Any]] = {}  # تخزين مؤقت للصفقات حسب ID
        self._running = False
        self._reconnect_attempts = 0
        self._max_reconnect_attempts = CONNECTION_SETTINGS["max_reconnect_attempts"]

        # Performance improvements
        self._message_batcher = MessageBatcher()
        self._connection_pool = ConnectionPool()
        self._rate_limiter = asyncio.Semaphore(10)  # Max 10 concurrent operations
        self._message_cache: Dict[str, Any] = {}
        self._cache_ttl = 5.0  # Cache TTL in seconds

        # Message processing optimization
        self._message_handlers = {
            "0": self._handle_initial_message,
            "2": self._handle_ping_message,
            "40": self._handle_connection_message,
            "451-[": self._handle_json_message_wrapper,
            "42": self._handle_auth_message,
            "[[5,": self._handle_payout_message,
            "{": self._handle_order_message, 
             "{": self._handle_checkwin2_message,
        }

    async def connect(self, urls: List[str], ssid: str) -> bool:
        """
        Connect to PocketOption WebSocket with fallback URLs

        Args:
            urls: List of WebSocket URLs to try
            ssid: Session ID for authentication

        Returns:
            bool: True if connected successfully
        """
        for url in urls:
            try:
                logger.info(f"Attempting to connect to {url}")

                # SSL context setup
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

                # Connect with timeout
                ws = await asyncio.wait_for(
                    websockets.connect(
                        url,
                        ssl=ssl_context,
                        extra_headers=DEFAULT_HEADERS,
                        ping_interval=CONNECTION_SETTINGS["ping_interval"],
                        ping_timeout=CONNECTION_SETTINGS["ping_timeout"],
                        close_timeout=CONNECTION_SETTINGS["close_timeout"],
                    ),
                    timeout=10.0,
                )
                self.websocket = ws  # type: ignore
                # Update connection info
                region = self._extract_region_from_url(url)
                self.connection_info = ConnectionInfo(
                    url=url,
                    region=region,
                    status=ConnectionStatus.CONNECTED,
                    connected_at=datetime.now(),
                    reconnect_attempts=self._reconnect_attempts,
                )

                logger.info(f"Connected to {region} region successfully")
                # Start message handling
                self._running = True

                # Send initial handshake and wait for completion
                await self._send_handshake(ssid)

                # Start background tasks after handshake is complete
                await self._start_background_tasks()

                self._reconnect_attempts = 0
                return True

            except Exception as e:
                logger.warning(f"Failed to connect to {url}: {e}")
                if self.websocket:
                    await self.websocket.close()
                    self.websocket = None
                continue

        raise ConnectionError("Failed to connect to any WebSocket endpoint")
    

    async def _handle_price_message(self, message: str):
        """
        Handle price messages and send them to parent client if available.
        """
        try:
            data = json.loads(message)  # [["EURUSD_otc",1757170634.115,1.16311]]

            for item in data:
                symbol = item[0]         # اسم الزوج
                timestamp = item[1]      # وقت (epoch)
                price = item[2]          # سعر

                # تحويل التايمستامب لوقت مقروء

                parsed = {
                    "symbol": symbol,
                    "time": timestamp,
                    "price": price
                }

                # لو في client مرتبط، نبعتله البيانات
                if self._parent_client3 is not None:
                    if asyncio.iscoroutinefunction(self._parent_client3.receive_price_data):
                        await self._parent_client3.receive_price_data(parsed)
                    else:
                        self._parent_client3.receive_price_data(parsed)

        except Exception as e:
            print(f"Error parsing price message: {e}")

    async def _handle_payout_message(self, message: str) -> None:
        try:
            # Decode the full message as JSON
            data: List[List[Any]] = json.loads(message)

            for i, asset_data in enumerate(data):


                if isinstance(asset_data, list) and len(asset_data) > 5:
                    try:
                        asset_id = asset_data[0]
                        asset_symbol = asset_data[1]
                        asset_name = asset_data[2]
                        asset_type = asset_data[3]
                        payout_percentage = asset_data[5]


                        payout_info = {
                            "id": asset_id,
                            "symbol": asset_symbol,
                            "name": asset_name,
                            "type": asset_type,
                            "payout": payout_percentage,
                        }


                        await self._emit_event("payout_update", payout_info)
                        
                        if self._parent_client:
                            self._parent_client._latest_payouts[asset_symbol] = payout_percentage

                    except IndexError:
                        print(f"IndexError: Incomplete data in asset: {asset_data}")
                    except Exception as inner_error:
                        print(f"Exception while processing asset #{i}: {inner_error}")
                else:
                    print(f"Unexpected format for asset data: {asset_data}") 

        except json.JSONDecodeError as decode_error:
            print(f"❌ JSON decoding failed: {decode_error}")
        except Exception as general_error:
            print(f"❌ General exception occurred: {general_error}")
            

    async def _handle_checkwin2_message(self, decoded_message: str) -> None:
        """
        استخراج بيانات الصفقة من رسالة WebSocket وطباعتها فقط.
        """
        try:
            data = json.loads(decoded_message)

            # تأكد أن البيانات تحتوي على صفقات
            if "deals" in data and isinstance(data["deals"], list) and data["deals"]:
                deal = data["deals"][0]

                    
                # استخراج البيانات المهمة
                deal_data = {
                    "ID": deal.get("id"),
                    "Asset": deal.get("asset"),
                    "Amount": deal.get("amount"),
                    "Profit": deal.get("profit"),
                    "PercentProfit": deal.get("percentProfit"),
                    "Open Time": deal.get("openTime"),
                    "Close Time": deal.get("closeTime"),
                    "Open Price": deal.get("openPrice"),
                    "Close Price": deal.get("closePrice"),
                    "Command": "CALL" if deal.get("command") == 1 else "PUT",
                    "Is Demo": bool(deal.get("isDemo")),
                    "Currency": deal.get("currency"),
                }

                # print("📊 بيانات الصفقة:")
                # for key, value in deal_data.items():
                #     print(f"  {key}: {value}")

                # إرسال الحدث في حال حبيت تستخدمه لاحقاً
                await self._emit_event("deal_info", deal_data)
                
                deal_id = deal.get("id")
                if deal_id:
                    self._deals_cache[str(deal_id)] = deal_data
                


        except Exception as e:
            logger.error(f"خطأ أثناء استخراج بيانات الصفقة: {e}")


    async def _handle_order_message(self, message: str) -> None:
        """
        معالجة رسالة صفقة تداول بصيغة JSON
        """
        try:
            data = json.loads(message)

            if "id" in data and "openTime" in data:
                order_info = {
                    "id": data["id"],
                    "asset": data.get("asset"),
                    "amount": data.get("amount"),
                    "profit": data.get("profit"),
                    "percentProfit": data.get("percentProfit"),
                    "currency": data.get("currency", "USD"),
                    "openTime": data.get("openTime"),
                    "closeTime": data.get("closeTime"),
                    "isDemo": bool(data.get("isDemo", 1)),
                }
                
                            
                logger.error(f"📦 تم استلام صفقة: {order_info}")
                await self._emit_event("order_info", order_info)
                
                if self._parent_client2:
                    self._parent_client2._latest_order_info = data["id"]
                    #print(f"🧾 القيمة الحالية في _latest_order_info: {self._parent_client2._latest_order_info}")




        except Exception as e:
            logger.error(f"❌ خطأ في _handle_order_message: {e}")



    async def disconnect(self):
        """Gracefully disconnect from WebSocket"""
        logger.info("Disconnecting from WebSocket")

        self._running = False

        # Cancel background tasks
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass

        # Close WebSocket connection
        if self.websocket:
            await self.websocket.close()
            self.websocket = None

        # Update connection status
        if self.connection_info:
            self.connection_info = ConnectionInfo(
                url=self.connection_info.url,
                region=self.connection_info.region,
                status=ConnectionStatus.DISCONNECTED,
                connected_at=self.connection_info.connected_at,
                reconnect_attempts=self.connection_info.reconnect_attempts,
            )

    async def send_message(self, message: str) -> None:
        """
        Send message to WebSocket

        Args:
            message: Message to send
        """
        if not self.websocket or self.websocket.closed:
            raise WebSocketError("WebSocket is not connected")

        try:
            await self.websocket.send(message)
            logger.debug(f"Sent message: {message}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise WebSocketError(f"Failed to send message: {e}")

    async def send_message_optimized(self, message: str) -> None:
        """
        Send message with batching optimization

        Args:
            message: Message to send
        """
        async with self._rate_limiter:
            if not self.websocket or self.websocket.closed:
                raise WebSocketError("WebSocket is not connected")

            try:
                start_time = time.time()

                # Add to batch
                batch = await self._message_batcher.add_message(message)

                # Send batch if ready
                if batch:
                    for msg in batch:
                        await self.websocket.send(msg)
                        logger.debug(f"Sent batched message: {msg}")

                # Update connection stats
                response_time = time.time() - start_time
                if self.connection_info:
                    await self._connection_pool.update_stats(
                        self.connection_info.url, response_time, True
                    )

            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                if self.connection_info:
                    await self._connection_pool.update_stats(
                        self.connection_info.url, 0, False
                    )
                raise WebSocketError(f"Failed to send message: {e}")

    async def receive_messages(self) -> None:
        """
        Continuously receive and process messages
        """
        try:
            while self._running and self.websocket:
                try:
                    message = await asyncio.wait_for(
                        self.websocket.recv(),
                        timeout=CONNECTION_SETTINGS["message_timeout"],
                    )
                    await self._process_message(message)

                except asyncio.TimeoutError:
                    logger.warning("Message receive timeout")
                    continue
                except ConnectionClosed:
                    logger.warning("WebSocket connection closed")
                    await self._handle_disconnect()
                    break

        except Exception as e:
            logger.error(f"Error in message receiving: {e}")
            await self._handle_disconnect()

    def add_event_handler(self, event: str, handler: Callable) -> None:
        """
        Add event handler

        Args:
            event: Event name
            handler: Handler function
        """
        if event not in self._event_handlers:
            self._event_handlers[event] = []
        self._event_handlers[event].append(handler)

    def remove_event_handler(self, event: str, handler: Callable) -> None:
        """
        Remove event handler

        Args:
            event: Event name
            handler: Handler function to remove
        """
        if event in self._event_handlers:
            try:
                self._event_handlers[event].remove(handler)
            except ValueError:
                pass

    async def _send_handshake(self, ssid: str) -> None:
        """Send initial handshake messages (following old API pattern exactly)"""
        try:
            # Wait for initial connection message with "0" and "sid" (like old API)
            logger.debug("Waiting for initial handshake message...")
            if not self.websocket:
                raise WebSocketError("WebSocket is not connected during handshake")
            initial_message = await asyncio.wait_for(
                self.websocket.recv(), timeout=10.0
            )
            logger.debug(f"Received initial: {initial_message}")

            # Ensure initial_message is a string
            if isinstance(initial_message, memoryview):
                initial_message = bytes(initial_message).decode("utf-8")
            elif isinstance(initial_message, (bytes, bytearray)):
                initial_message = initial_message.decode("utf-8")

            # Check if it's the expected initial message format
            if initial_message.startswith("0") and "sid" in initial_message:
                # Send "40" response (like old API)
                await self.send_message("40")
                logger.debug("Sent '40' response")

                # Wait for connection establishment message with "40" and "sid"
                conn_message = await asyncio.wait_for(
                    self.websocket.recv(), timeout=10.0
                )
                logger.debug(f"Received connection: {conn_message}")

                # Ensure conn_message is a string
                if isinstance(conn_message, memoryview):
                    conn_message_str = bytes(conn_message).decode("utf-8")
                elif isinstance(conn_message, (bytes, bytearray)):
                    conn_message_str = conn_message.decode("utf-8")
                else:
                    conn_message_str = conn_message
                if conn_message_str.startswith("40") and "sid" in conn_message_str:
                    # Send SSID authentication (like old API)
                    await self.send_message(ssid)
                    logger.debug("Sent SSID authentication")
                else:
                    logger.warning(
                        f"Unexpected connection message format: {conn_message}"
                    )
            else:
                logger.warning(f"Unexpected initial message format: {initial_message}")

            logger.debug("Handshake sequence completed")

        except asyncio.TimeoutError:
            logger.error("Handshake timeout - server didn't respond as expected")
            raise WebSocketError("Handshake timeout")
        except Exception as e:
            logger.error(f"Handshake failed: {e}")
            raise

    async def _start_background_tasks(self) -> None:
        """Start background tasks"""
        # Start ping task
        self._ping_task = asyncio.create_task(self._ping_loop())

        # Start message receiving task (only start it once here)
        asyncio.create_task(self.receive_messages())

    async def _ping_loop(self) -> None:
        """Send periodic ping messages"""
        while self._running and self.websocket:
            try:
                await asyncio.sleep(CONNECTION_SETTINGS["ping_interval"])

                if self.websocket and not self.websocket.closed:
                    await self.send_message('42["ps"]')

                    # Update last ping time
                    if self.connection_info:
                        self.connection_info = ConnectionInfo(
                            url=self.connection_info.url,
                            region=self.connection_info.region,
                            status=self.connection_info.status,
                            connected_at=self.connection_info.connected_at,
                            last_ping=datetime.now(),
                            reconnect_attempts=self.connection_info.reconnect_attempts,
                        )

            except Exception as e:
                logger.error(f"Ping failed: {e}")
                break
            
    def extract_pairs_info(data):
        result = []
        for item in data:
            if isinstance(item, list) and len(item) > 5:
                pair_name = item[1]
                display_name = item[2]
                percentage = item[5]
                result.append({
                    "pair": pair_name,
                    "name": display_name,
                    "percentage": percentage
                })
        return result


    async def _process_message(self, message) -> None:
        """
        Process incoming WebSocket message (following old API pattern exactly)

        Args:
            message: Raw message from WebSocket (bytes or str)
        """
        try:
            # Handle bytes messages first (like old API) - these contain balance data
            if isinstance(message, bytes):
                decoded_message = message.decode("utf-8")

                if decoded_message.startswith('[["'):
                    await self._handle_price_message(decoded_message)


                if decoded_message.strip().startswith("[[5,"):
                    await self._handle_payout_message(decoded_message)
                
                if decoded_message.strip().startswith("{") and '"id"' in decoded_message and '"openTime"' in decoded_message:
                    await self._handle_order_message(decoded_message)

                if decoded_message.strip().startswith("{") and '"deals"' in decoded_message:
                    await self._handle_checkwin2_message(decoded_message)

                try:
                    # Try to parse as JSON (like old API)
                    json_data = json.loads(decoded_message)
                    logger.debug(f"Received JSON bytes message: {json_data}")



                    
                    # Handle balance data (like old API)
                    if "balance" in json_data:
                        balance_data = {
                            "balance": json_data["balance"],
                            "currency": "USD",  # Default currency
                            "is_demo": bool(json_data.get("isDemo", 1)),
                        }
                        if "uid" in json_data:
                            balance_data["uid"] = json_data["uid"]

                        logger.info(f"Balance data received: {balance_data}")
                        await self._emit_event("balance_data", balance_data)

                    # Handle order data (like old API)
                    elif "requestId" in json_data and json_data["requestId"] == "buy":
                        await self._emit_event("order_data", json_data)
                        
                    
                    # Handle other JSON data
                    else:
                        await self._emit_event("json_data", json_data)

                except json.JSONDecodeError:
                    # If not JSON, treat as regular bytes message
                    logger.debug(f"Non-JSON bytes message: {decoded_message[:100]}...")

                return

            # Convert bytes to string if needed
            if isinstance(message, bytes):
                message = message.decode("utf-8")

            logger.debug(f"Received message: {message}")

            # Handle different message types
            if message.startswith("0") and "sid" in message:
                await self.send_message("40")

            elif message == "2":
                await self.send_message("3")

            elif message.startswith("40") and "sid" in message:
                # Connection established
                await self._emit_event("connected", {})

            elif message.startswith("451-["):
                # Parse JSON message
                json_part = message.split("-", 1)[1]
                data = json.loads(json_part)
                await self._handle_json_message(data)

            elif message.startswith("42") and "NotAuthorized" in message:
                logger.error("Authentication failed: Invalid SSID")
                await self._emit_event("auth_error", {"message": "Invalid SSID"})

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def _handle_initial_message(self, message: str) -> None:
        """Handle initial connection message"""
        if "sid" in message:
            await self.send_message("40")

    async def _handle_ping_message(self, message: str) -> None:
        """Handle ping message"""
        await self.send_message("3")

    async def _handle_connection_message(self, message: str) -> None:
        """Handle connection establishment message"""
        if "sid" in message:
            await self._emit_event("connected", {})

    async def _handle_json_message_wrapper(self, message: str) -> None:
        """Handle JSON message wrapper"""
        json_part = message.split("-", 1)[1]
        data = json.loads(json_part)
        await self._handle_json_message(data)

    async def _handle_auth_message(self, message: str) -> None:
        """Handle authentication message"""
        if "NotAuthorized" in message:
            logger.error("Authentication failed: Invalid SSID")
            await self._emit_event("auth_error", {"message": "Invalid SSID"})

    async def _process_message_optimized(self, message) -> None:
        """
        Process incoming WebSocket message with optimization

        Args:
            message: Raw message from WebSocket (bytes or str)
        """
        try:
            # Convert bytes to string if needed
            if isinstance(message, bytes):
                message = message.decode("utf-8")

            logger.debug(f"Received message: {message}")

            # Check cache first
            message_hash = hash(message)
            cached_time = self._message_cache.get(f"{message_hash}_time")

            if cached_time and time.time() - cached_time < self._cache_ttl:
                # Use cached processing result
                cached_result = self._message_cache.get(str(message_hash))
                if cached_result:
                    await self._emit_event("cached_message", cached_result)
                    return

            # Fast message routing
            for prefix, handler in self._message_handlers.items():
                if message.startswith(prefix):
                    await handler(message)
                    break
            else:
                # Unknown message type
                logger.warning(f"Unknown message type: {message[:20]}...")

            # Cache processing result
            self._message_cache[str(message_hash)] = {
                "processed": True,
                "type": "unknown",
            }
            self._message_cache[f"{message_hash}_time"] = time.time()

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def _handle_json_message(self, data: List[Any]) -> None:
        """
        Handle JSON formatted messages

        Args:
            data: Parsed JSON data
        """
        if not data or len(data) < 1:
            return

        event_type = data[0]
        event_data = data[1] if len(data) > 1 else {}

        # Handle specific events
        if event_type == "successauth":
            await self._emit_event("authenticated", event_data)

        elif event_type == "successupdateBalance":
            await self._emit_event("balance_updated", event_data)

        elif event_type == "successopenOrder":
            await self._emit_event("order_opened", event_data)

        elif event_type == "successcloseOrder":
            await self._emit_event("order_closed", event_data)

        elif event_type == "updateStream":
            await self._emit_event("stream_update", event_data)

        elif event_type == "loadHistoryPeriod":
            await self._emit_event("candles_received", event_data)

        elif event_type == "updateHistoryNew":
            await self._emit_event("history_update", event_data)

        else:
            await self._emit_event(
                "unknown_event", {"type": event_type, "data": event_data}
            )

    async def _emit_event(self, event: str, data: Dict[str, Any]) -> None:
        """
        Emit event to registered handlers

        Args:
            event: Event name
            data: Event data
        """
        if event in self._event_handlers:
            for handler in self._event_handlers[event]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(data)
                    else:
                        handler(data)
                except Exception as e:
                    logger.error(f"Error in event handler for {event}: {e}")
                    




    async def _handle_disconnect(self) -> None:
        """Handle WebSocket disconnection"""
        if self.connection_info:
            self.connection_info = ConnectionInfo(
                url=self.connection_info.url,
                region=self.connection_info.region,
                status=ConnectionStatus.DISCONNECTED,
                connected_at=self.connection_info.connected_at,
                last_ping=self.connection_info.last_ping,
                reconnect_attempts=self.connection_info.reconnect_attempts,
            )

        await self._emit_event("disconnected", {})

        # Attempt reconnection if enabled
        if self._reconnect_attempts < self._max_reconnect_attempts:
            self._reconnect_attempts += 1
            logger.info(
                f"Attempting reconnection {self._reconnect_attempts}/{self._max_reconnect_attempts}"
            )
            await asyncio.sleep(CONNECTION_SETTINGS["reconnect_delay"])
            # Note: Reconnection logic would be handled by the main client

    def _extract_region_from_url(self, url: str) -> str:
        """Extract region name from URL"""
        try:
            # Extract from URLs like "wss://api-eu.po.market/..."
            parts = url.split("//")[1].split(".")[0]
            if "api-" in parts:
                return parts.replace("api-", "").upper()
            elif "demo" in parts:
                return "DEMO"
            else:
                return "UNKNOWN"
        except Exception:
            return "UNKNOWN"

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected"""
        return (
            self.websocket is not None
            and not self.websocket.closed
            and self.connection_info is not None
            and self.connection_info.status == ConnectionStatus.CONNECTED
        )
