#   Copyright 2018 Braxton Mckee
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import hashlib
import struct
from typed_python._types import serialize


class Hash:
    def __init__(self, digest):
        self.digest = digest

    @staticmethod
    def from_integer(i):
        return Hash.from_string(struct.pack("!q", i))

    @staticmethod
    def from_float(f):
        return Hash.from_string(struct.pack("!d", f))

    @staticmethod
    def from_string(s):
        hasher = hashlib.sha1()
        if isinstance(s, str):
            s = s.encode("utf8")
        hasher.update(s)
        return Hash(hasher.digest())

    def __add__(self, other):
        assert isinstance(other, Hash)
        hasher = hashlib.sha1()
        hasher.update(self.digest)
        hasher.update(other.digest)
        return Hash(hasher.digest())

    @property
    def hexdigest(self):
        return self.digest.hex()

    def __str__(self):
        return "0x" + self.hexdigest

    def __repr__(self):
        return "0x" + self.hexdigest

    def __hash__(self):
        return hash(self.digest)

    def __lt__(self, other):
        return self.digest < other.digest

    def __eq__(self, other):
        return self.digest == other.digest


def sha_hash(val, serializationContext=None):
    if isinstance(val, tuple):
        h0 = Hash.from_integer(len(val))
        for i in val:
            h0 = h0 + sha_hash(i)
        return h0
    if isinstance(val, dict):
        return sha_hash(tuple(sorted(val.items())))
    if isinstance(val, int):
        return Hash.from_integer(val)
    if isinstance(val, float):
        return Hash.from_float(val)
    if isinstance(val, str):
        return Hash.from_string(val)
    if isinstance(val, bytes):
        return Hash.from_string(repr(val))
    if val is None:
        return Hash.from_string("")

    if hasattr(val, "__sha_hash__"):
        return val.__sha_hash__()

    return sha_hash(serialize(type(val), val, serializationContext))
