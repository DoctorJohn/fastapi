import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterable, Mapping, Optional, Union

from starlette.background import BackgroundTask
from starlette.concurrency import iterate_in_threadpool
from starlette.responses import (
    AsyncContentStream,
    Content,
    ContentStream,
    StreamingResponse,
)


@dataclass
class ServerSentEvent:
    comment: Optional[str] = None

    data: Optional[str] = None
    """
    The data of the event.
    """

    event: Optional[str] = None
    """
    The type of the event.

    As per SSE specification, the event type is "message" if not specified.
    """

    id: Optional[str] = None
    """
    An identifier for the event.

    Clients include this identifier in the Last-Event-ID header when reconnecting to the server.
    """

    retry: Optional[int] = None
    """
    Reconnection time, in milliseconds.

    Client's will attempt to reconnect after this timeout if the connection is lost.
    """


class EventSourceResponse(StreamingResponse):
    def __init__(
        self,
        content: ContentStream,
        keep_alive_interval: timedelta = timedelta(seconds=15),
        status_code: int = 200,
        headers: Optional[Mapping[str, str]] = None,
        background: Optional[BackgroundTask] = None,
    ) -> None:
        self.keep_alive_interval = keep_alive_interval

        merged_headers = {
            **(headers or {}),
            "Cache-Control": "no-store",
            "Connection": "keep-alive",
        }

        super().__init__(
            content=self.stream_sse_events(content),
            status_code=status_code,
            headers=merged_headers,
            media_type="text/event-stream",
            background=background,
        )

    async def stream_sse_events(self, content: ContentStream) -> AsyncContentStream:
        events = (
            content
            if isinstance(content, AsyncIterable)
            else iterate_in_threadpool(content)
        )

        queue = asyncio.Queue()

        async def queue_events():
            async for event in events:
                await queue.put(event)

        queue_events_task = asyncio.create_task(queue_events())

        try:
            while True:
                try:
                    event = await asyncio.wait_for(
                        queue.get(), timeout=self.keep_alive_interval.total_seconds()
                    )
                except asyncio.TimeoutError:
                    yield self.serialize_sse(ServerSentEvent(comment="keep-alive"))
                    continue

                yield self.serialize_sse(event)
        finally:
            queue_events_task.cancel()
            await queue_events_task

    def serialize_sse(self, event: Union[Content, ServerSentEvent]) -> str:
        if isinstance(event, ServerSentEvent):
            parts = []
            if event.comment is not None:
                parts.append(f": {event.comment}")
            if event.data is not None:
                parts.append(f"data: {event.data}")
            if event.event is not None:
                parts.append(f"event: {event.event}")
            if event.id is not None:
                parts.append(f"id: {event.id}")
            if event.retry is not None:
                parts.append(f"retry: {event.retry}")
            return "\n".join(parts) + "\n\n"
        return f"data: {event}\n\n"
