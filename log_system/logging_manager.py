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
import os
import glob
import sys

class LoggingSystem(object):
    def __init__(self, *args, **kwargs):
        self.log_path = kwargs.get("log_path")
        self.log_level = kwargs.get("level").upper()
        self.rotation = kwargs.get("rotation")
        self.retention = kwargs.get("retention")
        max_size = kwargs.get("max_size")
        self.max_size = self.parse_size(max_size)
        self.log_name = kwargs.get("log_name")
        self._configure_logger()        

    def _configure_logger(self):
        """
        Configures the logger for the application.
        This method sets up the logging configuration by creating the necessary log directory,
        removing the default logger, and adding new loggers with specified configurations.
        It also schedules a cleanup for old log files.
        Attributes:
            self.log_path (str): The directory path where log files will be stored.
            self.log_name (str): The base name of the log file.
            self.log_level (str): The logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL).
            self.rotation (str): The rotation policy for log files (e.g., "1 week", "500 MB").
            self.retention (str): The retention policy for log files (e.g., "10 days", "1 month").
        Log File:
            The log file will be created in the specified log path with the name format:
            "{log_name}_YYYY-MM-DD.log".
        Log Format:
            The log messages will follow the format:
            "[AiteaDistribution] | {log_name} | {time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}"
        Example:
            self._configure_logger()
        Raises:
            OSError: If the log directory cannot be created.
        """
        # Path to the log file
        os.makedirs(self.log_path, exist_ok=True)
        

        log_file = os.path.join(self.log_path, f"{self.log_name}" + "_{time:YYYY-MM-DD}.log")
        
        # Remove default logger
        logger.remove()
        # Add a new logger with the specified configurations
        logger.add(sys.stdout, serialize=False, level=self.log_level) # False - color, True without color
        logger.add(
            log_file,
            level=self.log_level,
            rotation=self.rotation,
            retention=self.retention,
            format= f"[AiteaDistribution] | {self.log_name} |"
                    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                   "<level>{message}</level>",
            colorize=True
        )        
        
        # Schedule a cleanup
        log_name = self.log_name.split('_')[0].capitalize()
        logger.add(lambda record: self.cleanup_logs)
        logger.info(f"{log_name} logs configured with level {self.log_level}. Logs will be stored in {self.log_path}")
    
    def parse_size(self, size_str):
        """Parses a human-readable file size string and converts it to bytes.

        Args:
            size_str (str): The file size string to parse (e.g., '10 MB', '5 GB').
        Raises:
            ValueError: If the size string format is invalid.

        Returns:
            int: The size in bytes.
        """
        size_str = size_str.upper().strip()
        size_splitted = size_str.split()
        size_value, size_units  = size_splitted[0], size_splitted[1]
        units = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
        for unit in units:
            if size_units.endswith(unit):
                return int(float(size_value.rstrip(unit).strip()) * units[unit])
        raise ValueError(f"Invalid size format: {size_value}")

    def get_folder_size(self):
        """Calculate the total size of the folder specified by log_path.
        This method walks through all directories and files within the specified 
        log_path and sums up their sizes to calculate the total size of the folder.

        Returns:
            int: The total size of the folder in bytes.
        """
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.log_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
        return total_size

    def cleanup_logs(self):
        """Cleans up old log files when the total log folder size exceeds the maximum allowed size.
        This method continuously checks the size of the log folder and removes the oldest log files
        until the total size is within the allowed limit.
        Returns:
            None
        """
        while self.get_folder_size() > self.max_size:
            log_files = sorted(glob.glob(os.path.join(self.log_path, "*.log")), key=os.path.getctime)
            if log_files:
                os.remove(log_files[0])
            else:
                break

# Example usage
if __name__ == "__main__":
    logging = {
        "level": "INFO",
        "rotation": "500 MB",
        "retention": "10 days",
        "max_size": "10 GB",
        "paths": {
            "aitea_sender": "/var/log/aitea/aitea_distribution/aitea_sender"
        }
    }
    logging['log_path'] = logging['paths']['aitea_sender']
    log_name = 'aitea_sender'
    LoggingSystem(log_name=log_name, **logging)
    # Sample log messages
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
