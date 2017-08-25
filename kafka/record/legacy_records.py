import io
import logging

from .abc import ABCRecord, ABCRecordBatch

from kafka.protocol.message import MessageSet

log = logging.getLogger(__name__)


class LegacyRecordBatch(ABCRecordBatch):

    def __init__(self, buffer):
        bytes_to_read = len(buffer)
        buffer = io.BytesIO(buffer)
        messages = MessageSet.decode(buffer, bytes_to_read=bytes_to_read)
        print(messages)
        assert len(messages) == 1
        self._message = messages[0]

    def validate_crc(self):
        return self._message[2].validate_crc()

    def __iter__(self):
        offset, size, msg = self._message
        if msg.is_compressed():
            # If relative offset is used, we need to decompress the entire
            # message first to compute the absolute offset.
            inner_mset = msg.decompress()
            if msg.magic > 0:
                last_offset, _, _ = inner_mset[-1]
                absolute_base_offset = offset - last_offset
            else:
                absolute_base_offset = -1

            for inner_offset, inner_size, inner_msg in inner_mset:
                # There should only ever be a single layer of compression
                assert not inner_msg.is_compressed(), (
                    'MessageSet at offset %d appears double-compressed. This '
                    'should not happen -- check your producers!' % offset)

                if msg.magic > 0:
                    # When magic value is greater than 0, the timestamp
                    # of a compressed message depends on the
                    # typestamp type of the wrapper message:
                    if msg.timestamp_type == 0:  # CREATE_TIME (0)
                        inner_timestamp = inner_msg.timestamp
                    else:  # LOG_APPEND_TIME (1)
                        inner_timestamp = msg.timestamp
                else:
                    inner_timestamp = 0

                if absolute_base_offset >= 0:
                    inner_offset += absolute_base_offset

                yield LegacyRecord(
                    inner_offset, inner_timestamp, msg.timestamp_type,
                    inner_msg.key, inner_msg.value, inner_msg.crc)
        else:
            yield LegacyRecord(offset, msg.timestamp, msg.timestamp_type,
                               msg.key, msg.value, msg.crc)


class LegacyRecord(ABCRecord):

    __slots__ = ("_offset", "_timestamp", "_timestamp_type", "_key", "_value",
                 "_crc")

    def __init__(self, offset, timestamp, timestamp_type, key, value, crc):
        self._offset = offset
        self._timestamp = timestamp
        self._timestamp_type = timestamp_type
        self._key = key
        self._value = value
        self._crc = crc

    @property
    def offset(self):
        return self._offset

    @property
    def timestamp(self):
        """ Epoch milliseconds
        """
        return self._timestamp

    @property
    def timestamp_type(self):
        """ CREATE_TIME(0) or APPEND_TIME(1)
        """
        return self._timestamp_type

    @property
    def key(self):
        """ Bytes key or None
        """
        return self._key

    @property
    def value(self):
        """ Bytes value or None
        """
        return self._value

    @property
    def headers(self):
        return []

    @property
    def checksum(self):
        return self._crc

    def __repr__(self):
        return (
            "LegacyRecord(offset={!r}, timestamp={!r}, timestamp_type={!r},"
            " key={!r}, value={!r}, crc={!r})".format(
                self._offset, self._timestamp, self._timestamp_type,
                self._key, self._value, self._crc)
        )
