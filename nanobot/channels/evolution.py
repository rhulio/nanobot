"""Evolution API channel implementation using webhook."""

import asyncio
import os
from aiohttp import web
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import EvolutionConfig


def _guess_mediatype(url: str) -> str:
    """Guess Evolution API mediatype from URL extension."""
    lower = url.lower().split("?")[0]
    if any(lower.endswith(ext) for ext in (".mp3", ".ogg", ".m4a", ".wav", ".opus")):
        return "audio"
    if any(lower.endswith(ext) for ext in (".mp4", ".mov", ".avi", ".mkv")):
        return "video"
    if any(lower.endswith(ext) for ext in (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip")):
        return "document"
    return "image"  # default


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
    
    @staticmethod
    def _normalize_phone(number: str) -> str:
        """Strip country code 55 and return digits only."""
        d = "".join(filter(str.isdigit, number))
        if d.startswith("55") and len(d) >= 12:
            d = d[2:]
        return d

    @staticmethod
    def _phones_match(a: str, b: str) -> bool:
        """
        Compare two BR phone numbers tolerantly:
        - ignores country code 55
        - ignores the 9th mobile digit transition (8-digit → 9-digit locals)
        """
        na = EvolutionChannel._normalize_phone(a)
        nb = EvolutionChannel._normalize_phone(b)
        if na == nb:
            return True
        if len(na) >= 10 and len(nb) >= 10:
            local_a, local_b = na[2:], nb[2:]
            if local_a.startswith("9") and local_a[1:] == local_b:
                return True
            if local_b.startswith("9") and local_b[1:] == local_a:
                return True
        return False

    def is_allowed(self, sender_id: str) -> bool:
        """Override to use tolerant BR phone number matching."""
        allow_list = getattr(self.config, "allow_from", [])
        if not allow_list:
            return True
        return any(self._phones_match(sender_id, entry) for entry in allow_list)

    async def start(self) -> None:
        """Start the Evolution webhook server."""
        port = self.config.port or 18791

        # Expose credentials as env vars so the evolution-api skill curl commands work
        default_instance = self.config.default_instance or next(iter(self._instances), "")
        instance_cfg = self._instances.get(default_instance, {})
        api_url = instance_cfg.get("api_url") or instance_cfg.get("apiUrl") or self.config.api_url
        api_key = instance_cfg.get("api_key") or instance_cfg.get("apiKey") or self.config.api_key
        if api_url:
            os.environ.setdefault("EVOLUTION_API_URL", api_url)
        if api_key:
            os.environ.setdefault("EVOLUTION_API_KEY", api_key)
        if default_instance:
            os.environ.setdefault("EVOLUTION_API_INSTANCE", default_instance)

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
        """Send a message through Evolution API (text, media, sticker, or reaction)."""
        import aiohttp

        # Resolve instance and credentials
        default_instance = self.config.default_instance or next(iter(self._instances), "")
        instance_name = msg.metadata.get("instance_name", default_instance)
        instance_config = self._instances.get(instance_name, {})
        api_url = (
            instance_config.get("api_url") or instance_config.get("apiUrl")
            or self.config.api_url or os.getenv("EVOLUTION_API_URL", "")
        )
        api_key = (
            instance_config.get("api_key") or instance_config.get("apiKey")
            or self.config.api_key or os.getenv("EVOLUTION_API_KEY", "")
        )

        if not api_url or not api_key:
            logger.error(f"Evolution API not configured for instance {instance_name}")
            return

        headers = {"apikey": api_key, "Content-Type": "application/json"}

        # Normalize destination number (strip JID suffix, add BR country code if needed)
        to_jid = msg.chat_id
        to_number = to_jid.split("@")[0] if "@" in to_jid else to_jid
        to_number = "".join(filter(str.isdigit, to_number))
        if len(to_number) in (10, 11) and not to_number.startswith("55"):
            to_number = "55" + to_number

        try:
            async with aiohttp.ClientSession() as session:
                # Reaction — send emoji reaction to a specific message
                if msg.metadata.get("reaction"):
                    reaction_key = msg.metadata.get("reaction_key", {
                        "remoteJid": to_jid if "@" in to_jid else f"{to_number}@s.whatsapp.net",
                        "fromMe": False,
                        "id": msg.metadata.get("message_id", ""),
                    })
                    payload = {"key": reaction_key, "reaction": msg.metadata["reaction"]}
                    url = f"{api_url}/message/sendReaction/{instance_name}"
                    async with session.post(url, headers=headers, json=payload) as resp:
                        if resp.status < 400:
                            logger.info(f"Sent reaction via Evolution API to {to_number}")
                        else:
                            logger.error(f"Failed to send reaction: {resp.status} - {await resp.text()}")

                # Sticker
                elif msg.metadata.get("sticker"):
                    payload = {"number": to_number, "sticker": msg.metadata["sticker"]}
                    url = f"{api_url}/message/sendSticker/{instance_name}"
                    async with session.post(url, headers=headers, json=payload) as resp:
                        if resp.status < 400:
                            logger.info(f"Sent sticker via Evolution API to {to_number}")
                        else:
                            logger.error(f"Failed to send sticker: {resp.status} - {await resp.text()}")

                # Media (image, video, audio, document)
                elif msg.media:
                    media_url = msg.media[0]
                    mediatype = msg.metadata.get("mediatype", _guess_mediatype(media_url))
                    payload: dict = {
                        "number": to_number,
                        "mediatype": mediatype,
                        "media": media_url,
                        "delay": 1200,
                    }
                    if msg.content:
                        payload["caption"] = msg.content
                    if mediatype == "document" and msg.metadata.get("fileName"):
                        payload["fileName"] = msg.metadata["fileName"]
                    url = f"{api_url}/message/sendMedia/{instance_name}"
                    async with session.post(url, headers=headers, json=payload) as resp:
                        if resp.status < 400:
                            logger.info(f"Sent {mediatype} via Evolution API to {to_number}")
                        else:
                            logger.error(f"Failed to send media: {resp.status} - {await resp.text()}")

                # Plain text
                else:
                    payload = {"number": to_number, "text": msg.content, "delay": 1200}
                    url = f"{api_url}/message/sendText/{instance_name}"
                    async with session.post(url, headers=headers, json=payload) as resp:
                        if resp.status < 400:
                            logger.info(f"Sent message via Evolution API to {to_number}")
                        else:
                            logger.error(f"Failed to send message: {resp.status} - {await resp.text()}")

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

        event_type = data.get("event", "")
        logger.info(f"Evolution webhook received: event={event_type!r} instance={instance_name!r}")

        # Only process actual message events — ignore connection updates, QR codes, etc.
        if event_type and event_type not in ("messages.upsert", "messages.update", ""):
            logger.info(f"Ignoring non-message event: {event_type}")
            return web.json_response({"status": "ignored"})

        # Get instance config
        instance_config = self._instances.get(instance_name, {})
        allowlist = instance_config.get("allow_from", self.config.allow_from)

        try:
            # Extract message data — Evolution v2 wraps in array, v1 sends object directly
            raw_data = data.get("data", {})
            if isinstance(raw_data, list):
                msg_data = raw_data[0] if raw_data else {}
            else:
                msg_data = raw_data

            logger.info(f"Evolution msg_data keys: {list(msg_data.keys()) if isinstance(msg_data, dict) else type(msg_data).__name__}")

            key = msg_data.get("key", {})

            # Get sender info - handle different formats
            remote_jid = key.get("remoteJid", "")
            participant = key.get("participant", "")

            if not remote_jid:
                logger.debug(f"Ignoring event with no remoteJid: {event_type!r}")
                return web.json_response({"status": "ignored"})

            # Ignore messages sent by the bot itself
            if key.get("fromMe", False):
                return web.json_response({"status": "ignored"})

            # Extract phone number from JID
            if "@s.whatsapp.net" in remote_jid:
                sender_id = remote_jid.split("@")[0]
            elif "@g.us" in remote_jid:
                # Group message
                sender_id = participant.split("@")[0] if participant else remote_jid.split("@")[0]
            else:
                sender_id = remote_jid

            sender_normalized = self._normalize_phone(sender_id)

            # Check allowlist
            if allowlist and not any(self._phones_match(sender_id, entry) for entry in allowlist):
                logger.warning(f"Blocked message from {sender_id} ({sender_normalized}) - not in allowlist")
                return web.json_response({"status": "blocked"})

            # Use normalized form as sender_id for consistent session/allowlist keys
            sender_id = self._normalize_phone(sender_id) or sender_id

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
            elif not msg_type:
                logger.debug("Ignoring event with empty message payload")
                return web.json_response({"status": "ignored"})
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

        except Exception as e:
            logger.error(f"Error processing Evolution webhook: {e}", exc_info=True)

        return web.json_response({"status": "ok"})
    
    def add_instance(self, instance_name: str, api_url: str, api_key: str, allow_from: list[str] | None = None) -> None:
        """Add an Evolution API instance configuration."""
        self._instances[instance_name] = {
            "api_url": api_url,
            "api_key": api_key,
            "allow_from": allow_from or []
        }
        logger.info(f"Added Evolution instance: {instance_name}")
