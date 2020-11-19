# Copyright 2018 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = [
    'Error', 'BadRequest', 'MessageError', 'ReadError', 'CreateError',
    'DeleteError', 'ExecuteError', 'WriteError', 'HistoryError', 'VersionError'
]


class Error(Exception):
    """v3io_frames Exception"""


class BadRequest(Exception):
    """An error in query"""


class MessageError(Error):
    """An error in message"""


class ReadError(Error):
    """An error in read"""


class WriteError(Error):
    """An error in write"""


class CreateError(Error):
    """An error in table creation"""


class DeleteError(Error):
    """An error in table deletion"""


class ExecuteError(Error):
    """An error in executing command"""


class HistoryError(Error):
    """An error in querying history logs"""


class VersionError(Error):
    """An error in getting server version"""
