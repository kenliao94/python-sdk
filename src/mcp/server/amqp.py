"""
AMQP Server Transport Module

This module provides functionality for creating an AMQP-based transport layer
that can be used to communicate with an MCP client through RabbitMQ.

Example usage:
```
    async def run_server():
        async with amqp_server("amqp://localhost", "mcp_requests", "mcp_responses") as (read_stream, write_stream):
            # read_stream contains incoming JSONRPCMessages from RabbitMQ
            # write_stream allows sending JSONRPCMessages to RabbitMQ
            server = await create_my_server()
            await server.run(read_stream, write_stream, init_options)

    anyio.run(run_server)
```
"""

import ssl
from contextlib import asynccontextmanager

import aio_pika
import anyio
import anyio.lowlevel
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

import mcp.types as types
from mcp.shared.message import SessionMessage


@asynccontextmanager
async def amqp_server(
    host: str,
    port: int,
    username: str,
    password: str,
    name: str,
    request_queue: str = "mcp-input",
    response_queue: str = "mcp-output",
):
    """
    Server transport for AMQP: this communicates with an MCP client by reading
    from a RabbitMQ request queue and writing to a response queue.
    """
    read_stream: MemoryObjectReceiveStream[SessionMessage | Exception]
    read_stream_writer: MemoryObjectSendStream[SessionMessage | Exception]

    write_stream: MemoryObjectSendStream[SessionMessage]
    write_stream_reader: MemoryObjectReceiveStream[SessionMessage]

    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    url = f"amqps://{username}:{password}@{host}:{port}"
    
    # Create SSL context for secure connection
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
    
    # Connect using aio-pika
    connection = await aio_pika.connect_robust(url, ssl_context=ssl_context)
    channel = await connection.channel()
    
    # Declare the exchange
    topic_exchange = await channel.declare_exchange(
        "mcp", 
        aio_pika.ExchangeType.TOPIC, 
        durable=True
    )
    
    # Declare queues
    request_q = await channel.declare_queue(request_queue, durable=True)
    await channel.declare_queue(response_queue, durable=True)

    # Bind the queues
    await request_q.bind(topic_exchange, routing_key=f"mcp.{name}.request")

    async def amqp_reader():
        try:
            async with read_stream_writer:
                async with request_q.iterator() as queue_iter:
                    async for message in queue_iter:
                        try:
                            json_message = types.JSONRPCMessage.model_validate_json(message.body.decode('utf-8'))
                            session_message = SessionMessage(json_message)
                            await read_stream_writer.send(session_message)
                            await message.ack()
                        except Exception as exc:
                            await read_stream_writer.send(exc)
                            await message.nack(requeue=False)
        except anyio.ClosedResourceError:
            await anyio.lowlevel.checkpoint()

    async def amqp_writer():
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    json_data = session_message.message.model_dump_json(by_alias=True, exclude_none=True)
                    await topic_exchange.publish(
                        aio_pika.Message(
                            json_data.encode('utf-8'),
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                        ),
                        routing_key=f"mcp.{name}.response"
                    )
        except anyio.ClosedResourceError:
            await anyio.lowlevel.checkpoint()

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(amqp_reader)
            tg.start_soon(amqp_writer)
            yield read_stream, write_stream
    finally:
        await connection.close()