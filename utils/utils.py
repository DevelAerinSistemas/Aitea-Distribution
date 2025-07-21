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

import json

def read_json_conf(path: str) -> dict:
    """Method to read a JSON file

    Args:
        path (str): path of the JSON

    Returns:
        dict: dictionary with the JSON data
    """
    with open(path) as f:
        data = json.load(f)
    return data