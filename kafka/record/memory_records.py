# This class takes advantage of the fact that all formats v0, v1 and v2 of
# messages storage has the same byte offsets for Length and Magic fields.
# Lets look closely at what leading bytes all versions have:
#
# V0 and V1 (Offset is MessageSet part, other bytes are Message ones):
#  Offset => Int64
#  BytesLength => Int32
#  CRC => Int32
#  Magic => Int8
#  ...
#
# V2:
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  ...
#
# So we can iterate over batches just by knowing offsets of Length. Magic is
# used to construct the correct class for Batch itself.

import struct

from kafka.errors import CorruptRecordException
from kafka.protocol.message import MessageSet
from .abc import ABCRecords
from .legacy_records import LegacyRecordBatch
from .util import make_slice


class MemoryRecords(ABCRecords):

    LENGTH_OFFSET = struct.calcsize(">q")
    LOG_OVERHEAD = struct.calcsize(">qi")
    MAGIC_OFFSET = struct.calcsize(">qii")

    # Minimum space requirements for Record V0
    RECORD_OVERHEAD_V0 = struct.calcsize(
        ">i"  # CRC
        "b"  # Magic
        "b"  # Attributes
        "i"  # Key size
        "i"  # Value size
    )

    def __init__(self, bytes_data):
        self._buffer = bytes_data
        self._slices, self._remaining = self._split_slices(bytes_data)
        self._next_slice = 0

    def as_message_set(self):
        len_batch = len(self._buffer)
        return MessageSet.decode(self._buffer, bytes_to_read=len_batch)

    def size_in_bytes(self):
        return len(self._buffer)

    def valid_bytes(self):
        return len(self._buffer) - self._remaining

    def has_next(self):
        return len(self._slices) > self._next_slice

    def next_batch(self):
        if not self.has_next():
            return None
        buffer = self._slices[self._next_slice]
        self._next_slice += 1
        magic, = struct.unpack_from(">b", buffer, self.MAGIC_OFFSET)
        if magic >= 2:
            raise NotImplementedError("V2 not supported yet")
        else:
            return LegacyRecordBatch(buffer)

    @classmethod
    def _split_slices(cls, bytes_data):
        len_offset = cls.LENGTH_OFFSET
        log_overhead = cls.LOG_OVERHEAD
        record_overhead = cls.RECORD_OVERHEAD_V0
        buffer_len = len(bytes_data)
        next_slice = 0
        remaining = 0
        slices = []
        while next_slice != buffer_len:
            remaining = buffer_len - next_slice
            if remaining and remaining < log_overhead:
                # Will be re-checked in Fetcher for remaining bytes
                break

            length, = struct.unpack_from(
                ">i", bytes_data, next_slice + len_offset)
            if length < record_overhead:
                raise CorruptRecordException(
                    "Record size is less than the minimum record overhead "
                    "({})".format(record_overhead))

            slice_end = next_slice + length + log_overhead
            if slice_end > buffer_len:
                # Will be re-checked in Fetcher for remaining bytes
                break

            buffer = make_slice(bytes_data, next_slice, slice_end)
            slices.append(buffer)

            next_slice = slice_end
        return slices, remaining