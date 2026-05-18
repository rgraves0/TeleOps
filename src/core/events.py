from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Awaitable
from typing import Callable
from typing import Any

logger = logging.getLogger(__name__)


# =========================================================
# EVENT
# =========================================================


@dataclass
class Event:

    event_id: str

    name: str

    payload: dict

    created_at: str = field(
        default_factory=lambda:
        datetime.utcnow()
        .isoformat()
    )


# =========================================================
# EVENT HANDLER TYPE
# =========================================================


EventHandler = Callable[
    [Event],
    Awaitable[None],
]


# =========================================================
# EVENT BUS
# =========================================================


class EventBus:

    def __init__(
        self,
        queue_limit: int = 1000,
        max_concurrent_handlers: int = 10,
    ) -> None:

        self.queue: asyncio.Queue = (
            asyncio.Queue(
                maxsize=queue_limit
            )
        )

        self.subscribers: dict[
            str,
            list[EventHandler]
        ] = defaultdict(list)

        self.running = False

        self.worker_task = None

        self.max_concurrent_handlers = (
            max_concurrent_handlers
        )

        self.semaphore = (
            asyncio.Semaphore(
                max_concurrent_handlers
            )
        )

        logger.info(
            "EventBus initialized"
        )

    # =====================================================
    # START
    # =====================================================

    async def start(
        self,
    ) -> None:

        if self.running:
            return

        self.running = True

        self.worker_task = (
            asyncio.create_task(
                self._event_loop()
            )
        )

        logger.info(
            "EventBus started"
        )

    # =====================================================
    # STOP
    # =====================================================

    async def stop(
        self,
    ) -> None:

        self.running = False

        if self.worker_task:

            self.worker_task.cancel()

            try:

                await (
                    self.worker_task
                )

            except asyncio.CancelledError:
                pass

        logger.warning(
            "EventBus stopped"
        )

    # =====================================================
    # SUBSCRIBE
    # =====================================================

    def subscribe(
        self,
        event_name: str,
        handler: EventHandler,
    ) -> None:

        self.subscribers[
            event_name
        ].append(handler)

        logger.info(
            "Subscribed handler=%s "
            "event=%s",
            handler.__name__,
            event_name,
        )

    # =====================================================
    # UNSUBSCRIBE
    # =====================================================

    def unsubscribe(
        self,
        event_name: str,
        handler: EventHandler,
    ) -> None:

        if (
            event_name
            not in self.subscribers
        ):

            return

        try:

            self.subscribers[
                event_name
            ].remove(handler)

        except ValueError:
            pass

    # =====================================================
    # EMIT EVENT
    # =====================================================

    async def emit(
        self,
        event_name: str,
        payload: dict | None = None,
    ) -> Event:

        event = Event(

            event_id=str(
                uuid.uuid4()
            ),

            name=event_name,

            payload=payload or {},
        )

        try:

            await self.queue.put(
                event
            )

            logger.debug(
                "Event queued=%s",
                event_name,
            )

        except asyncio.QueueFull:

            logger.error(
                "Event queue full"
            )

        return event

    # =====================================================
    # LOOP
    # =====================================================

    async def _event_loop(
        self,
    ) -> None:

        while self.running:

            try:

                event = (
                    await self.queue.get()
                )

                await self._dispatch(
                    event
                )

            except asyncio.CancelledError:

                break

            except Exception:

                logger.exception(
                    "Event loop failed"
                )

    # =====================================================
    # DISPATCH
    # =====================================================

    async def _dispatch(
        self,
        event: Event,
    ) -> None:

        handlers = (
            self.subscribers.get(
                event.name,
                []
            )
        )

        if not handlers:

            logger.debug(
                "No subscribers "
                "for event=%s",
                event.name,
            )

            return

        logger.info(
            "Dispatching event=%s "
            "handlers=%s",
            event.name,
            len(handlers),
        )

        tasks = [

            asyncio.create_task(
                self._safe_execute(
                    handler,
                    event,
                )
            )

            for handler
            in handlers
        ]

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

    # =====================================================
    # SAFE EXECUTION
    # =====================================================

    async def _safe_execute(
        self,
        handler: EventHandler,
        event: Event,
    ) -> None:

        async with self.semaphore:

            try:

                await handler(
                    event
                )

            except Exception:

                logger.exception(
                    (
                        "Event handler failed "
                        "handler=%s "
                        "event=%s"
                    ),
                    handler.__name__,
                    event.name,
                )

    # =====================================================
    # STATS
    # =====================================================

    def stats(
        self,
    ) -> dict:

        return {

            "running":
            self.running,

            "queue_size":
            self.queue.qsize(),

            "subscribers":
            {
                key: len(value)
                for key, value
                in self.subscribers.items()
            },
        }


# =========================================================
# GLOBAL EVENT BUS
# =========================================================


event_bus = EventBus()
