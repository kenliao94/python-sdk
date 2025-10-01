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
from pydantic import BaseModel

import mcp.types as types
from mcp.shared.message import SessionMessage


class OAuth(BaseModel):
    access_token: str

class BasicAuth(BaseModel):
    username: str
    password: str

class AMQPSettings(BaseModel):
    host: str
    port: int
    exchange_name: str
    auth: OAuth | BasicAuth | None = None

async def __connect_amqp(amqp_settings: AMQPSettings):
    auth = amqp_settings.auth
    username = None
    password = None
    
    if isinstance(auth, OAuth): 
        username = ""
        password = auth.access_token
    elif isinstance(auth, BasicAuth):
        username = auth.username
        password = auth.password
    else:
        raise ValueError("Unsupported auth settings {auth}")

    url = f"amqps://{username}:{password}@{amqp_settings.host}:{amqp_settings.port}"
    
    # Create SSL context for secure connection
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
    
    # Connect using aio-pika
    connection = await aio_pika.connect_robust(url, ssl_context=ssl_context)
    channel = await connection.channel()
    
    return connection, channel

async def declare_queue(amqp_settings: AMQPSettings):
    pass

async def publish_to_queue(amqp_settings: AMQPSettings):
    pass

@asynccontextmanager
async def amqp_server(
    name: str,
    amqp_settings: AMQPSettings
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
    
    connection, channel = await __connect_amqp(amqp_settings=amqp_settings)
    
    # Declare the exchange
    topic_exchange = await channel.declare_exchange(
        amqp_settings.exchange_name, 
        aio_pika.ExchangeType.TOPIC, 
        durable=True
    )
    
    # Declare queues
    request_queue_name = f"mcp-{name}-request"
    request_queue = await channel.declare_queue(request_queue_name, durable=True)

    # Bind the queues
    from_client_routing_key = f"mcp.{name}.request"
    to_client_routing_key = f"mcp.{name}.response"
    await request_queue.bind(topic_exchange, routing_key=from_client_routing_key)

    # TODO(ken) use a queue to queue up request-response for different client
    async def amqp_reader():
        try:
            async with read_stream_writer:
                async with request_queue.iterator() as queue_iter:
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
                        routing_key=to_client_routing_key
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