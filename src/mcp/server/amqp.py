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

import anyio
import anyio.lowlevel
import pika
import pika.adapters.asyncio_connection
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

import mcp.types as types
from mcp.shared.message import SessionMessage


class RabbitMQConnection:
    def __init__(self, host: str, port: int, username: str, password: str, use_tls: bool, stream_port: int = 5552):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.protocol = "amqps" if use_tls else "amqp"
        self.url = f"{self.protocol}://{username}:{password}@{host}:{port}"
        self.parameters = pika.URLParameters(self.url)
        self.stream_port = stream_port

        if use_tls:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
            self.parameters.ssl_options = pika.SSLOptions(context=ssl_context)

    def get_channel(self) -> tuple[pika.BlockingConnection, pika.channel.Channel]:
        connection = pika.BlockingConnection(self.parameters)
        channel = connection.channel()
        return connection, channel

@asynccontextmanager
async def amqp_server(
    host: str,
    port: int,
    username: str,
    password: str,
    request_queue: str = "mcp-input",
    response_queue: str = "mcp-output",
    exchange: str = "",
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
    parameters = pika.URLParameters(url)
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
    parameters.ssl_options = pika.SSLOptions(context=ssl_context)

    connection = await pika.adapters.asyncio_connection.AsyncioConnection.create(parameters)
    channel = await connection.channel()
    
    await channel.queue_declare(queue=request_queue, durable=True)
    await channel.queue_declare(queue=response_queue, durable=True)

    async def amqp_reader():
        try:
            async with read_stream_writer:
                async for method, properties, body in channel.consume(request_queue):
                    try:
                        message = types.JSONRPCMessage.model_validate_json(body.decode('utf-8'))
                        session_message = SessionMessage(message)
                        await read_stream_writer.send(session_message)
                        await channel.basic_ack(method.delivery_tag)
                    except Exception as exc:
                        await read_stream_writer.send(exc)
                        await channel.basic_nack(method.delivery_tag, requeue=False)
        except anyio.ClosedResourceError:
            await anyio.lowlevel.checkpoint()

    async def amqp_writer():
        try:
            async with write_stream_reader:
                async for session_message in write_stream_reader:
                    json_data = session_message.message.model_dump_json(by_alias=True, exclude_none=True)
                    await channel.basic_publish(
                        exchange=exchange,
                        routing_key=response_queue,
                        body=json_data.encode('utf-8'),
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
        except anyio.ClosedResourceError:
            await anyio.lowlevel.checkpoint()

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(amqp_reader)
            tg.start_soon(amqp_writer)
            yield read_stream, write_stream
    finally:
        await channel.close()
        await connection.close()