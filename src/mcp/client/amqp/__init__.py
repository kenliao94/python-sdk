import ssl
from contextlib import asynccontextmanager

import aio_pika
import anyio
import anyio.lowlevel
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic import BaseModel

import mcp.types as types
from mcp.shared.message import SessionMessage


class AmqpServerParameters(BaseModel):
    host: str
    """The RabbitMQ host to connect to."""
    
    port: int = 5671
    """The RabbitMQ port (default: 5671 for AMQPS)."""
    
    username: str
    """Username for RabbitMQ authentication."""
    
    password: str
    """Password for RabbitMQ authentication."""
    
    name: str
    """Server name for queue naming."""


@asynccontextmanager
async def amqp_client(server: AmqpServerParameters):
    """
    Client transport for AMQP: this will connect to a server by communicating
    with it over RabbitMQ queues.
    """
    read_stream: MemoryObjectReceiveStream[SessionMessage | Exception]
    read_stream_writer: MemoryObjectSendStream[SessionMessage | Exception]

    write_stream: MemoryObjectSendStream[SessionMessage]
    write_stream_reader: MemoryObjectReceiveStream[SessionMessage]

    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    url = f"amqps://{server.username}:{server.password}@{server.host}:{server.port}"
    
    # Create SSL context for secure connection
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers("ECDHE+AESGCM:!ECDSA")
    
    try:
        # Connect using aio-pika
        connection = await aio_pika.connect_robust(url, ssl_context=ssl_context)
        channel = await connection.channel()
        
        # Declare the exchange
        topic_exchange = await channel.declare_exchange(
            "mcp", 
            aio_pika.ExchangeType.TOPIC, 
            durable=True
        )
        
        # Declare queues (client reads from response, writes to request)
        request_queue = f"mcp-{server.name}-request"
        response_queue = f"mcp-{server.name}-response"
        request_q = await channel.declare_queue(request_queue, durable=True)
        response_q = await channel.declare_queue(response_queue, durable=True)

        # Bind the queues
        await request_q.bind(topic_exchange, routing_key=f"mcp.{server.name}.request")
        await response_q.bind(topic_exchange, routing_key=f"mcp.{server.name}.response")

    except Exception:
        # Clean up streams if connection fails
        await read_stream.aclose()
        await write_stream.aclose()
        await read_stream_writer.aclose()
        await write_stream_reader.aclose()
        raise

    async def amqp_reader():
        try:
            async with read_stream_writer:
                async with response_q.iterator() as queue_iter:
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
                        routing_key=f"mcp.{server.name}.request"
                    )
        except anyio.ClosedResourceError:
            await anyio.lowlevel.checkpoint()

    async with anyio.create_task_group() as tg:
        tg.start_soon(amqp_reader)
        tg.start_soon(amqp_writer)
        try:
            yield read_stream, write_stream
        finally:
            await connection.close()
            await read_stream.aclose()
            await write_stream.aclose()
            await read_stream_writer.aclose()
            await write_stream_reader.aclose()