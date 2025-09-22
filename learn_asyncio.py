import asyncio

# This creates an event loop and indefinitely cycles through
# its collection of jobs.
print("hi")
event_loop = asyncio.new_event_loop()
event_loop.run_forever()