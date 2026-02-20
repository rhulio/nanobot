"""Evolution API channel implementation using webhook."""

import asyncio
import os
from aiohttp import web
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import EvolutionConfig


class EvolutionChannel(BaseChannel):
    """
    Evolution API channel using webhook.
    
    Receives messages from Evolution API via HTTP webhook and forwards
    to the nanobot message bus.
    """
    
    name = "evolution"
    
    def __init__(self, config: EvolutionConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: EvolutionConfig = config
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        # Populate instances from config so routes are created on start()
        self._instances: dict[str, dict] = dict(config.instances)
    
    async def start(self) -> None:
        """Start the Evolution webhook server."""
        port = self.config.port or 18791
        
        self._app = web.Application()
        
        # Fixed webhook route — Evolution API must be configured to POST to /webhook/evolution
        self._app.router.add_post("/webhook/evolution", self._handle_webhook_fixed)
        self._app.router.add_get("/health/evolution", self._health_check_fixed)

        # Health check general
        self._app.router.add_get("/health", self._health_check_general)
        
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        
        self._site = web.TCPSite(self._runner, "0.0.0.0", port)
        await self._site.start()
        
        self._running = True
        logger.info(f"Evolution API webhook server started on port {port} — POST /webhook/evolution")
        
        # Keep running
        while self._running:
            await asyncio.sleep(1)
    
    async def stop(self) -> None:
        """Stop the Evolution webhook server."""
        self._running = False
        
        if self._runner:
            await self._runner.cleanup()
        
        logger.info("Evolution API webhook server stopped")
    
    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Evolution API."""
        import aiohttp
        
        # Get instance from metadata or use default
        default_instance = self.config.default_instance or next(iter(self._instances), "")
        instance_name = msg.metadata.get("instance_name", default_instance)
        
        # Get instance config — fall back to top-level api_url/api_key then env vars
        instance_config = self._instances.get(instance_name, {})
        api_url = instance_config.get("api_url") or instance_config.get("apiUrl") or self.config.api_url or os.getenv("EVOLUTION_API_URL", "")
        api_key = instance_config.get("api_key") or instance_config.get("apiKey") or self.config.api_key or os.getenv("EVOLUTION_API_KEY", "")
        
        if not api_url or not api_key:
            logger.error(f"Evolution API not configured for instance {instance_name}")
            return
        
        # Extract phone number from chat_id (remove @s.whatsapp.net or @g.us)
        to_number = msg.chat_id.split("@")[0] if "@" in msg.chat_id else msg.chat_id
        
        # Normalize number
        to_number = "".join(filter(str.isdigit, to_number))
        if len(to_number) == 10 or len(to_number) == 11:
            if not to_number.startswith("55"):
                to_number = "55" + to_number
        
        headers = {
            "apikey": api_key,
            "Content-Type": "application/json"
        }
        
        url = f"{api_url}/message/sendText/{instance_name}"
        payload = {
            "number": to_number,
            "text": msg.content,
            "delay": 1200
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as resp:
                    if resp.status < 400:
                        logger.info(f"Sent message via Evolution API to {to_number}")
                    else:
                        error = await resp.text()
                        logger.error(f"Failed to send message: {resp.status} - {error}")
        except Exception as e:
            logger.error(f"Error sending via Evolution API: {e}")
    
    async def _health_check_fixed(self, request: web.Request) -> web.Response:
        """Health check for the fixed /health/evolution route."""
        instance_name = self.config.default_instance or next(iter(self._instances), "evolution")
        return await self._health_check(request, instance_name)

    async def _handle_webhook_fixed(self, request: web.Request) -> web.Response:
        """Handle webhook on the fixed /webhook/evolution route."""
        instance_name = self.config.default_instance or next(iter(self._instances), "evolution")
        return await self._handle_webhook(request, instance_name)

    async def _health_check_dynamic(self, request: web.Request) -> web.Response:
        """Health check for any instance (catch-all route)."""
        instance_name = request.match_info["instance_name"]
        return await self._health_check(request, instance_name)

    async def _handle_webhook_dynamic(self, request: web.Request) -> web.Response:
        """Handle webhook for any instance (catch-all route)."""
        instance_name = request.match_info["instance_name"]
        return await self._handle_webhook(request, instance_name)

    async def _health_check(self, request: web.Request, instance_name: str) -> web.Response:
        """Health check for specific instance."""
        return web.json_response({
            "status": "ok",
            "instance": instance_name
        })
    
    async def _health_check_general(self, request: web.Request) -> web.Response:
        """General health check."""
        return web.json_response({
            "status": "ok",
            "channel": "evolution",
            "instances": list(self._instances.keys())
        })
    
    async def _handle_webhook(self, request: web.Request, instance_name: str) -> web.Response:
        """Handle incoming webhook from Evolution API."""
        try:
            data = await request.json()
        except Exception as e:
            logger.error(f"Failed to parse webhook data: {e}")
            return web.json_response({"error": "Invalid JSON"}, status=400)
        
        # Get instance config
        instance_config = self._instances.get(instance_name, {})
        allowlist = instance_config.get("allow_from", self.config.allow_from)
        
        # Extract message data
        msg_data = data.get("data", {})
        key = msg_data.get("key", {})
        
        # Get sender info - handle different formats
        remote_jid = key.get("remoteJid", "")
        participant = key.get("participant", "")
        
        # Extract phone number from JID
        if "@s.whatsapp.net" in remote_jid:
            sender_id = remote_jid.split("@")[0]
        elif "@g.us" in remote_jid:
            # Group message
            sender_id = participant.split("@")[0] if participant else remote_jid.split("@")[0]
        else:
            sender_id = remote_jid
        
        # Check allowlist
        if allowlist and sender_id not in allowlist:
            logger.warning(f"Blocked message from {sender_id} - not in allowlist")
            return web.json_response({"status": "blocked"})
        
        # Get message content
        msg_type = msg_data.get("message", {})
        
        if "conversation" in msg_type:
            content = msg_type["conversation"]
        elif "extendedTextMessage" in msg_type:
            content = msg_type["extendedTextMessage"].get("text", "")
        elif "imageMessage" in msg_type:
            content = msg_type["imageMessage"].get("caption", "[Image]")
        elif "videoMessage" in msg_type:
            content = msg_type["videoMessage"].get("caption", "[Video]")
        elif "documentMessage" in msg_type:
            content = msg_type["documentMessage"].get("caption", "[Document]")
        elif "audioMessage" in msg_type:
            content = "[Audio]"
        elif "stickerMessage" in msg_type:
            content = "[Sticker]"
        else:
            content = str(msg_type)
        
        # Build chat_id for replies (use full JID)
        chat_id = remote_jid
        
        logger.info(f"Evolution message from {sender_id}: {content[:50]}...")
        
        # Forward to message bus
        await self._handle_message(
            sender_id=sender_id,
            chat_id=chat_id,
            content=content,
            metadata={
                "message_id": key.get("id", ""),
                "instance_name": instance_name,
                "timestamp": msg_data.get("messageTimestamp", ""),
                "push_name": msg_data.get("pushName", ""),
                "is_group": "@g.us" in remote_jid
            }
        )
        
        return web.json_response({"status": "ok"})
    
    def add_instance(self, instance_name: str, api_url: str, api_key: str, allow_from: list[str] | None = None) -> None:
        """Add an Evolution API instance configuration."""
        self._instances[instance_name] = {
            "api_url": api_url,
            "api_key": api_key,
            "allow_from": allow_from or []
        }
        logger.info(f"Added Evolution instance: {instance_name}")
