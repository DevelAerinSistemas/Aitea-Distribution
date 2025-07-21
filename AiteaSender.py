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

from core.transfer_manager import RedisFileTransfer


class AiteaSender:
    """
    AiteaSender is responsible for listening to Redis pubsub channels and handling file transfer commands.

    Attributes:
        transfer (RedisFileTransfer): An instance of RedisFileTransfer to manage file transfers.
        ps (PubSub): A Redis pubsub instance subscribed to the 'file_channel'.
    """
    def __init__(self):
        transfer_manager_type = "sender"
        self.transfer = RedisFileTransfer(transfer_manager_type=transfer_manager_type)
        channel_to_listen_sender = self.transfer.redis_config_sender.get('channel_to_listen', 'files_to_send')
        self.key_to_publish = self.transfer.redis_config_receiver.get('key_to_publish', 'files_to_receive')
        self.ps = self.transfer.redis_sender.pubsub()
        self.ps.subscribe(channel_to_listen_sender)
        

    def listen(self) -> None:
        """
        This method listens for incoming messages from a Redis pubsub channel.
        When a message is received, it checks if the message type is 'message' and processes
        commands that start with "ORDER SEND_FILES". The commands are expected to contain
        filenames, and the method delegates file transfer operations to the RedisFileTransfer instance.
        """
        logger.info(f"[AD]{self.__class__.__name__} - Listening for SEND_FILES orders...")
        for message in self.ps.listen():
            try:
                logger.info(f"[AD]{self.__class__.__name__} - Received message: {message}")
                if message['type'] != 'message':
                    continue

                command = message['data'].decode()
                if command.startswith("ORDER SEND_FILES"):
                    self.manage_command(command)
            except Exception as e:
                logger.error(f"[AD] - An error occurred: {e}")

    def manage_command(self, command: str) -> None:
        """
        Processes the command received from the Redis pubsub channel.

        Args:
            command (str): The command string containing the filenames to be sent.
        """
        _, _, *files = command.split()
        for fname in files:
            fname_splitted = fname.split('.')
            ext = fname_splitted[-1]
            if ext == 'json':
                file_type = 'json'
            elif ext == 'pkl':
                file_type = 'pkl'
            elif ext == 'txt':
                file_type = 'txt'
            elif ext == 'so':
                file_type = 'so'
            else:
                file_type = 'bin'
            file_name = fname_splitted[0].split('/')[-1]
            if file_type == 'so':
                file_name = fname.split('/')[-1].rsplit('.',1)[0]
            self.transfer.send_file(fname, self.key_to_publish, file_type, file_name)

if __name__ == "__main__":
    asender = AiteaSender()
    asender.listen()