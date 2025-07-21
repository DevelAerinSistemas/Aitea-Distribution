'''
 # @ Project: Aitea-Distribution
 # @ Author: Aerin S.L.
 # @ Create Time: 2025-05-22 18:48:26
 # @ Description:
 # @ Version: 1.0.0
 # @ -------:
 # @ Modified by: Aerin S.L.
 # @ Modified time: 2025-05-22 19:19:31
 # @ License: This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY
; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 '''

from loguru import logger
import json

from core.transfer_manager import RedisFileTransfer


class AiteaReceiver:
    """
    AiteaSender is responsible for listening to Redis pubsub channels and handling file transfer commands.

    Attributes:
        transfer (RedisFileTransfer): An instance of RedisFileTransfer to manage file transfers.
        ps (PubSub): A Redis pubsub instance subscribed to the 'file_channel'.
    """
    def __init__(self):
        transfer_manager_type = "receiver"
        self.transfer = RedisFileTransfer(transfer_manager_type=transfer_manager_type)
        self.ps = self.transfer.redis_receiver.pubsub()
        channel_to_listen_receiver = self.transfer.redis_config_receiver.get('channel_to_listen', 'files_to_send')
        self.key_to_check = self.transfer.redis_config_receiver.get('key_to_publish', 'files_to_receive')
        self.ps.subscribe(channel_to_listen_receiver)

    def listen(self) -> None:
        """
        This method listens for incoming messages from a Redis pubsub channel.
        When a message is received, it checks if the message type is 'message' and processes
        commands that start with "ORDER SEND_FILES". The commands are expected to contain
        filenames, and the method delegates file transfer operations to the RedisFileTransfer instance.
        """
        logger.info(f"[AD]{self.__class__.__name__} - Listening for new messages as receiver...")
        for message in self.ps.listen():
            try:
                logger.info(f"[AD]{self.__class__.__name__} - Received message: {message}")
                data = message.get("data", None)
                if data is not None:
                    if isinstance(data, bytes) and data != 1:
                        data = data.decode('utf-8')
                    if data != 1:
                        data = json.loads(data)
                        key_to_verify = data.get("key_to_verify", None)
                        file_type = data.get("file_type", None)
                        file_name = data.get("file_name", "")
                        self.transfer.receive_file(key_to_verify, file_type, file_name)

            except Exception as e:
                logger.error(f"[AD] - An error occurred: {e}")

if __name__ == "__main__":
    areceiver = AiteaReceiver()
    areceiver.listen()