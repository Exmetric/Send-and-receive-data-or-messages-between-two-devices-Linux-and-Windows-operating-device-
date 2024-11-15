import asyncio
import json
import logging
import socket
import time
from typing import Optional, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UDPCommunicator:
   def __init__(self, local_ip: str, local_port: int, remote_ip: str, remote_port: int):
       self.local_ip = local_ip
       self.local_port = local_port
       self.remote_ip = remote_ip
       self.remote_port = remote_port
       self.transport: Optional[asyncio.DatagramTransport] = None
       self.message_callback: Optional[Callable] = None
       self.is_running = False
       self._socket = None

   def set_message_callback(self, callback: Callable):
       """Set callback function to handle received messages"""
       self.message_callback = callback

   class CommunicationProtocol(asyncio.DatagramProtocol):
       def __init__(self, callback: Optional[Callable] = None):
           self.callback = callback
           self.transport = None

       def connection_made(self, transport):
           self.transport = transport
           logger.info("UDP Connection established")

       def datagram_received(self, data, addr):
           try:
               message = json.loads(data.decode())
               logger.info(f"Received from {addr}: {message}")
               if self.callback:
                   self.callback(message, addr)
           except json.JSONDecodeError:
               logger.error(f"Failed to decode message: {data}")
           except Exception as e:
               logger.error(f"Error processing message: {e}")

   def _create_and_bind_socket(self):
       """Create and bind a socket with proper options"""
       sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
       sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
       
       # Try to bind to the port
       max_retries = 5
       retry_delay = 1
       
       for attempt in range(max_retries):
           try:
               sock.bind((self.local_ip, self.local_port))
               logger.info(f"Successfully bound to port {self.local_port}")
               return sock
           except OSError as e:
               if attempt < max_retries - 1:
                   logger.warning(f"Port {self.local_port} is busy, retrying in {retry_delay} seconds...")
                   time.sleep(retry_delay)
               else:
                   raise
       
       raise OSError(f"Failed to bind to port {self.local_port} after {max_retries} attempts")

   async def start(self):
       """Start the UDP communication"""
       try:
           loop = asyncio.get_event_loop()
           
           # Create and bind the socket
           self._socket = self._create_and_bind_socket()
           
           # Create endpoint using the bound socket
           transport, _ = await loop.create_datagram_endpoint(
               lambda: self.CommunicationProtocol(self.message_callback),
               sock=self._socket
           )
           
           self.transport = transport
           self.is_running = True
           logger.info(f"UDP Communicator started on {self.local_ip}:{self.local_port}")
           
       except Exception as e:
           logger.error(f"Failed to start UDP communication: {e}")
           if self._socket:
               self._socket.close()
           raise

   async def send_message(self, message: dict):
       """Send a message to the remote endpoint"""
       if not self.transport:
           logger.error("Transport not initialized. Call start() first.")
           return
       
       try:
           data = json.dumps(message).encode()
           self.transport.sendto(data, (self.remote_ip, self.remote_port))
           logger.info(f"Sent to {self.remote_ip}:{self.remote_port}: {message}")
       except Exception as e:
           logger.error(f"Failed to send message: {e}")

   def stop(self):
       """Stop the UDP communication"""
       if self.transport:
           self.transport.close()
       if self._socket:
           self._socket.close()
       self.is_running = False
       logger.info("UDP Communicator stopped")

async def message_handler(message: dict, addr):
   """Example message handler"""
   logger.info(f"Handling message from {addr}: {message}")
   # Add your message handling logic here

async def main():
   # Linux device configuration (swapped ports and IPs)
   local_ip = "0.0.0.0"  # Listen on all available interfaces
   local_port = 7532     # This was the remote_port in Windows operating device code, and now its local port on here.
   remote_ip = "xxx.xxx.xx.xxx"  # Replace with your Windows operating device IP address, check with command prompt ipconfig and check the ipv4 address.
   remote_port = 7531    # This was the local_port in Windows operating device code, and now its remote port on here.

   # Create UDP communicator
   communicator = UDPCommunicator(local_ip, local_port, remote_ip, remote_port)
   communicator.set_message_callback(message_handler)
   
   try:
       # Start communication
       await communicator.start()

       # Keep the connection alive
       while communicator.is_running:
           await asyncio.sleep(1)

   except KeyboardInterrupt:
       logger.info("Shutting down...")
   finally:
       communicator.stop()

if __name__ == "__main__":
   # No need for Windows-specific event loop policy on a Linux operating device
   asyncio.run(main())
