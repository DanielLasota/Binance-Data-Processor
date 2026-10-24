# from __future__ import annotations
#
# import struct
# from multiprocessing.shared_memory import SharedMemory
#
# # ── Memory layout ─────────────────────────────────────────────────────────────
# #  [0  : 8 ]  write_pos  — logical uint64, monotonically increasing, PRODUCER-owned
# #  [8  : 16]  read_pos   — logical uint64, monotonically increasing, CONSUMER-owned
# #  [16 : 16+capacity]  ring data
# #
# # Message framing inside ring data:
# #  [1B  cid        ]  0 or 1 (client id)
# #  [8B  timestamp  ]  uint64 little-endian
# #  [4B  msg_len    ]  uint32 little-endian
# #  [msg_len bytes  ]  utf-8 payload
# # ─────────────────────────────────────────────────────────────────────────────
#
# _HEADER_SIZE = 16
# _META_SIZE   = 13        # 1 (cid) + 8 (ts) + 4 (msg_len)
# _WP_OFFSET   = 0
# _RP_OFFSET   = 8
#
# _pack_meta   = struct.Struct('<BQI')   # module-level, compiled once
# _unpack_meta = struct.Struct('<BQI')
#
#
# class SharedRingBuffer:
#
#     __slots__ = ('_shm', '_buf', '_capacity', '_mask')
#
#     def __init__(self, shm: SharedMemory, capacity: int) -> None:
#         if capacity & (capacity - 1):
#             raise ValueError(f"capacity must be power of 2, got {capacity}")
#         self._shm      = shm
#         self._buf      = shm.buf
#         self._capacity = capacity
#         self._mask     = capacity - 1
#
#     def __getstate__(self) -> dict:
#         return {'name': self._shm.name, 'capacity': self._capacity}
#
#     def __setstate__(self, state: dict) -> None:
#         shm = SharedMemory(name=state['name'], create=False)
#         self._shm      = shm
#         self._buf      = shm.buf
#         self._capacity = state['capacity']
#         self._mask     = state['capacity'] - 1
#
#     # ── internal ──────────────────────────────────────────────────────────────
#
#     def _wp(self) -> int:
#         return struct.unpack_from('<Q', self._buf, _WP_OFFSET)[0]
#
#     def _rp(self) -> int:
#         return struct.unpack_from('<Q', self._buf, _RP_OFFSET)[0]
#
#     def _set_wp(self, val: int) -> None:
#         struct.pack_into('<Q', self._buf, _WP_OFFSET, val)
#
#     def _set_rp(self, val: int) -> None:
#         struct.pack_into('<Q', self._buf, _RP_OFFSET, val)
#
#     def _write_raw(self, logical_pos: int, data: bytes) -> None:
#         n      = len(data)
#         offset = logical_pos & self._mask
#         end    = offset + n
#         base   = _HEADER_SIZE
#
#         if end <= self._capacity:
#             self._buf[base + offset : base + end] = data
#         else:
#             pivot = self._capacity - offset
#             self._buf[base + offset : base + self._capacity] = data[:pivot]
#             self._buf[base          : base + n - pivot      ] = data[pivot:]
#
#     def _read_raw(self, logical_pos: int, n: int) -> bytes:
#         offset = logical_pos & self._mask
#         end    = offset + n
#         base   = _HEADER_SIZE
#
#         if end <= self._capacity:
#             return bytes(self._buf[base + offset : base + end])
#
#         pivot = self._capacity - offset
#         return (bytes(self._buf[base + offset : base + self._capacity])
#               + bytes(self._buf[base           : base + n - pivot     ]))
#
#     # ── public API ────────────────────────────────────────────────────────────
#
#     def put_nowait(self, msg: str, ts: int, cid: int) -> bool:
#         data  = msg.encode()          # utf-8, single alloc
#         total = _META_SIZE + len(data)
#
#         wp   = self._wp()
#         free = self._capacity - (wp - self._rp())
#
#         if total > free:
#             return False
#
#         self._write_raw(wp, _pack_meta.pack(cid, ts, len(data)))
#         self._write_raw(wp + _META_SIZE, data)
#         self._set_wp(wp + total)
#         return True
#
#     def get_nowait(self) -> tuple[int, int, str] | None:
#         wp = self._wp()
#         rp = self._rp()
#
#         if wp - rp < _META_SIZE:
#             return None
#
#         cid, ts, msg_len = _unpack_meta.unpack(self._read_raw(rp, _META_SIZE))
#         rp += _META_SIZE
#
#         if wp - rp < msg_len:
#             return None
#
#         msg = self._read_raw(rp, msg_len).decode()
#         self._set_rp(rp + msg_len)
#         return cid, ts, msg
#
#     # ── utils ─────────────────────────────────────────────────────────────────
#
#     def empty(self) -> bool:
#         return self._wp() == self._rp()
#
#     def qsize(self) -> int:
#         return self._wp() - self._rp()
#
#     def clear(self) -> None:
#         self._set_wp(0)
#         self._set_rp(0)
#
#     @classmethod
#     def create(cls, name: str, capacity: int) -> 'SharedRingBuffer':
#         shm = SharedMemory(name=name, create=True, size=_HEADER_SIZE + capacity)
#         shm.buf[:_HEADER_SIZE] = b'\x00' * _HEADER_SIZE
#         return cls(shm, capacity)
#
#     @classmethod
#     def attach(cls, name: str, capacity: int) -> 'SharedRingBuffer':
#         shm = SharedMemory(name=name, create=False)
#         return cls(shm, capacity)
#
#     def close(self) -> None:
#         self._buf.release()
#         self._shm.close()
#
#     def unlink(self) -> None:
#         self._shm.unlink()

from __future__ import annotations

import struct
import sys
from multiprocessing.shared_memory import SharedMemory

_META = struct.Struct("<BQI")
_pack_into = _META.pack_into
_unpack_from = _META.unpack_from


class SharedRingBuffer:
    """Single serialized producer, single consumer; little-endian host.

    Same layout: uint64 write/read positions, then a byte ring.
    Python memory access does NOT provide portable acquire/release atomics.
    clear() requires both endpoints to be stopped.
    """

    __slots__ = ("_shm", "_buf", "_pos", "_capacity", "_mask")

    @staticmethod
    def _validate(capacity):
        if sys.byteorder != "little":
            raise RuntimeError("This implementation requires little-endian")

        if capacity < 16 or capacity & (capacity - 1):
            raise ValueError("capacity must be a power of two >= 16")

    def __init__(self, shm: SharedMemory, capacity: int):
        self._validate(capacity)

        if shm.size < 16 + capacity:
            raise ValueError(
                "Shared memory is smaller than requested capacity"
            )

        self._shm = shm
        self._buf = shm.buf
        self._pos = shm.buf[:16].cast("Q")
        self._capacity = capacity
        self._mask = capacity - 1

    def put_nowait(self, msg: str, ts: int, cid: int) -> bool:
        data = msg.encode("utf-8")
        n = len(data)
        total = 13 + n

        pos = self._pos
        wp = pos[0]
        capacity = self._capacity

        if total > capacity - (wp - pos[1]):
            return False

        offset = wp & self._mask
        buf = self._buf

        if offset + total <= capacity:
            start = 16 + offset
            _pack_into(buf, start, cid, ts, n)
            buf[start + 13:start + total] = data
        else:
            self._write_raw(wp, _META.pack(cid, ts, n))
            self._write_raw(wp + 13, data)

        pos[0] = wp + total
        return True

    def get_nowait(self) -> tuple[int, int, str] | None:
        pos = self._pos
        rp = pos[1]
        available = pos[0] - rp

        if available < 13:
            return None

        offset = rp & self._mask
        buf = self._buf
        capacity = self._capacity

        if offset + 13 <= capacity:
            cid, ts, n = _unpack_from(buf, 16 + offset)
        else:
            cid, ts, n = _META.unpack(self._read_raw(rp, 13))

        total = 13 + n

        if total > capacity:
            raise RuntimeError("Invalid record length in shared memory")

        if total > available:
            return None

        if offset + total <= capacity:
            msg = bytes(
                buf[16 + offset + 13:16 + offset + total]
            ).decode("utf-8")
        else:
            msg = self._read_raw(rp + 13, n).decode("utf-8")

        # Release space only after copying/decoding the payload.
        pos[1] = rp + total
        return cid, ts, msg

    def _write_raw(self, logical_pos: int, data: bytes):
        offset = logical_pos & self._mask
        n = len(data)
        first = min(n, self._capacity - offset)

        self._buf[16 + offset:16 + offset + first] = data[:first]

        if first < n:
            self._buf[16:16 + n - first] = data[first:]

    def _read_raw(self, logical_pos: int, n: int) -> bytes:
        offset = logical_pos & self._mask
        first = min(n, self._capacity - offset)

        data = bytes(self._buf[16 + offset:16 + offset + first])

        if first < n:
            data += bytes(self._buf[16:16 + n - first])

        return data

    def empty(self) -> bool:
        return self._pos[0] == self._pos[1]

    def qsize(self) -> int:
        """Occupied bytes, not number of messages; approximate under concurrency."""
        return self._pos[0] - self._pos[1]

    def clear(self):
        """Call only when producer and consumer are stopped."""
        self._pos[0] = 0
        self._pos[1] = 0

    @classmethod
    def create(cls, name: str, capacity: int):
        cls._validate(capacity)
        shm = SharedMemory(
            name=name,
            create=True,
            size=16 + capacity,
        )

        try:
            shm.buf[:16] = bytes(16)
            return cls(shm, capacity)
        except BaseException:
            shm.close()
            shm.unlink()
            raise

    @classmethod
    def attach(cls, name: str, capacity: int):
        cls._validate(capacity)
        shm = SharedMemory(name=name, create=False)

        try:
            return cls(shm, capacity)
        except BaseException:
            shm.close()
            raise

    def __getstate__(self):
        return {
            "name": self._shm.name,
            "capacity": self._capacity,
        }

    def __setstate__(self, state):
        shm = SharedMemory(name=state["name"], create=False)

        try:
            self.__init__(shm, state["capacity"])
        except BaseException:
            shm.close()
            raise

    def close(self):
        self._pos.release()
        self._buf.release()
        self._shm.close()

    def unlink(self):
        self._shm.unlink()
