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

import redis
import zlib
import os
import json
import pickle
import sys
from loguru import logger
from typing import Any

from log_system.logging_manager import LoggingSystem
from utils.utils import read_json_conf

DEFAULT_PATH = "/opt/aitea_distribution/"


class RedisFileTransfer:
    @logger.catch
    def __init__(self, compress=True, transfer_manager_type: str = "sender"):
        self.redis_config_sender = None
        self.redis_config_receiver = None
        self.redis_sender = None
        self.redis_receiver = None
        self.redis_config = {}
        global_config = self.load_global_config()
        self.transfer_manager_type = transfer_manager_type
        self.initialize_logging(global_config)
        self.initialize_connections(global_config)
        self.paths_to_save = self.get_paths_to_save(global_config)
        self.compress = compress

        logger.info(
            f"[AD]{self.__class__.__name__} - Initialized in {self.transfer_manager_type} mode")

    @logger.catch
    def load_global_config(self) -> dict:
        """
        Loads the global configuration from a JSON file.

        Returns:
            dict: A dictionary containing the global configuration settings.
        """
        global_config = read_json_conf("config/global_config.json")
        return global_config

    @logger.catch
    def initialize_logging(self, global_config: dict) -> None:
        """
        Initializes the logging system using the provided global configuration.

        Args:
            global_config (dict): A dictionary containing global configuration settings,
                      including logging paths and options.
        """
        logging_config = global_config.get("logging", {})
        log_path = global_config.get("paths", {}).get(
            "aitea_sender", "/var/log/aitea/aitea_distribution/aitea_sender/")
        logging_config.setdefault("log_path", log_path)
        LoggingSystem(log_name="Aitea Distribution", **logging_config)

    @logger.catch
    def get_paths_to_save(self, global_config: dict) -> dict:
        """
        Retrieves the paths to save from the global configuration.

        Args:
            global_config (dict): A dictionary containing the global configuration settings.

        Returns:
            dict: A dictionary containing the paths to save, extracted from the global configuration.
        """
        paths = global_config.get("paths_to_save", {})
        return paths

    @logger.catch
    def initialize_connections(self, global_config: dict) -> None:
        """
        Initializes the Redis connections based on the global configuration.

        Args:
            global_config (dict): A dictionary containing global configuration settings,
                      including connection details for Redis.
        """
        connections_path = global_config.get(
            "connections_path", "config/connections.json")
        connections = read_json_conf(connections_path)
        redis_configs = connections.get("redis", {})

        self.redis_config_sender = {}
        self.redis_config_receiver = {}

        if self.transfer_manager_type == "sender":
            self.redis_config_sender = redis_configs.get("sender", {})
            self.redis_config_receiver = redis_configs.get("receiver", {})
        else:
            self.redis_config_receiver = redis_configs.get(
                self.transfer_manager_type, {})

        if self.transfer_manager_type == "sender":
            self.redis_sender = self._create_redis_client(
                self.redis_config_sender, role="sender")
            self.redis_receiver = self._create_redis_client(
                self.redis_config_receiver, role="receiver")
        elif self.transfer_manager_type == "receiver":
            self.redis_receiver = self._create_redis_client(
                self.redis_config_receiver, role="receiver")

        if not getattr(self, 'redis_receiver', None) and not getattr(self, 'redis_sender', None):
            logger.error(
                f"[AD]{self.__class__.__name__} - No valid Redis configuration found in {connections_path}.")
            sys.exit(1)

    @logger.catch
    def _create_redis_client(self, config: dict, role: str) -> Any:
        """
        Creates a Redis client using the provided configuration.

        Args:
            config (dict): A dictionary containing Redis connection details such as host, port, and password.
            role (str): The role of the Redis client (e.g., 'sender' or 'receiver').

        Returns:
            Any: A Redis client instance if the connection is successful, otherwise None.
        """
        if not config:
            logger.warning(
                f"[AD]{self.__class__.__name__} - Missing Redis config for {role}.")
            return None
        try:
            client = redis.Redis(
                host=config.get('host'),
                port=config.get('port'),
                password=config.get('password')
            )
            if client.ping():
                logger.info(
                    f"[AD]{self.__class__.__name__} - Initialized Redis connection for {role} with config: {config}"
                )
                return client
        except Exception as e:
            logger.error(
                f"[AD]{self.__class__.__name__} - Failed to connect to Redis ({role}): {e}")
        return None

    @logger.catch
    def _compress(self, data: bytes) -> bytes:
        """
        Compresses the given data using zlib if compression is enabled.

        Args:
            data (bytes): The data to be compressed.

        Returns:
            bytes: The compressed data if compression is enabled, otherwise the original data.
        """
        return zlib.compress(data) if self.compress else data

    @logger.catch
    def _decompress(self, data: bytes) -> bytes:
        """
        Decompresses the given data if compression is enabled.

        Args:
            data (bytes): The compressed data to be decompressed.

        Returns:
            bytes: The decompressed data if compression is enabled; 
               otherwise, returns the original data unchanged.
        """
        return zlib.decompress(data) if self.compress else data

    @logger.catch
    def send_file(self, file_path: str, key: str, file_type: str = 'bin', file_name: str = "") -> None:
        """
        Sends a file to the Redis receiver.

        Args:
            file_path (str): The path to the file to be sent.
            key (str): The Redis key under which the file will be stored.
            file_type (str, optional): The type of the file (e.g., 'bin', 'json', 'pkl', 'txt', 'so'). Defaults to 'bin'.
            file_name (str, optional): The name of the file to be stored. Defaults to an empty string.
        """
        if file_type == 'json':
            with open(file_path, 'r') as f:
                data = json.load(f)
                serialized = json.dumps(data).encode('utf-8')
        elif file_type == 'pkl':
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
                serialized = pickle.dumps(data)
        elif file_type == 'txt':
            with open(file_path, 'r') as f:
                serialized = f.read().encode('utf-8')
        elif file_type == 'so':
            if not file_name:
                file_name = os.path.basename(file_path)
            if not file_name.endswith('.so'):
                file_name += '.so'
            with open(file_path, 'rb') as f:
                serialized = f.read()
        else:
            with open(file_path, 'rb') as f:
                serialized = f.read()
            file_type = 'bin'  # Default to binary for other file types

        compressed = self._compress(serialized)
        self.redis_receiver.set(key, compressed)
        message = {"key_to_verify": key,
                   "file_type": file_type, "file_name": file_name}
        message = json.dumps(message).encode(
            'utf-8') if isinstance(message, dict) else message
        self.redis_receiver.publish(
            self.redis_config_receiver.get("channel_to_listen"), message)
        logger.info(
            f"[AD]{self.__class__.__name__} - {file_type.upper()} file '{file_path}' has been sent and stored in Redis key '{key}'")

    @logger.catch
    def receive_file(self, key: str, file_type: str = 'bin', file_name: str = "") -> None:
        """
        Receives a file from the Redis receiver and saves it locally.

        Args:
            key (str): The Redis key under which the file is stored.
            file_type (str, optional): The type of the file (e.g., 'bin', 'json', 'pkl', 'txt', 'so'). Defaults to 'bin'.
            file_name (str, optional): The name of the file to be saved. Defaults to an empty string.
        """
        raw_name = file_name + "." + file_type
        data = self.redis_receiver.get(key)
        if data is None:
            logger.error(
                f"[AD]{self.__class__.__name__} - No data found for key '{key}'")
            return

        decompressed = self._decompress(data)

        if file_type == 'json':
            folder_path = self.paths_to_save.get("json", DEFAULT_PATH)
            raw_name = os.path.join(folder_path, raw_name)
            obj = json.loads(decompressed.decode('utf-8'))
            with open(raw_name, 'w') as f:
                json.dump(obj, f)
        elif file_type == 'pkl':
            folder_path = self.paths_to_save.get("pkl", DEFAULT_PATH)
            raw_name = os.path.join(folder_path, raw_name)
            obj = pickle.loads(decompressed)
            with open(raw_name, 'wb') as f:
                pickle.dump(obj, f)
        elif file_type == 'txt':
            folder_path = self.paths_to_save.get("txt", DEFAULT_PATH)
            raw_name = os.path.join(folder_path, raw_name)
            with open(raw_name, 'w') as f:
                f.write(decompressed.decode('utf-8'))
        elif file_type == 'so':
            if not file_name:
                file_name = os.path.basename(raw_name)
            if not file_name.endswith('.so'):
                file_name += '.so'
            folder_path = self.paths_to_save.get("so", DEFAULT_PATH)
            raw_name = os.path.join(folder_path, file_name)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            if not raw_name.endswith('.so'):
                raw_name += '.so'
            with open(raw_name, 'wb') as f:
                f.write(decompressed)
            os.chmod(raw_name, 0o755)

        else:
            raw_name = os.path.join("/opt/", raw_name)
            with open(raw_name, 'wb') as f:
                f.write(decompressed)

        logger.info(
            f"[AD]{self.__class__.__name__} - {file_type.upper()} file from key '{key}' has been received and saved to '{raw_name}'")


if __name__ == "__main__":
    rft = RedisFileTransfer()
    rft.send_file('test_to_send.txt', 'key_test_to_send', file_type='bin')
