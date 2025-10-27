from __future__ import annotations

from pysync.rolling_checksum import RollingChecksum


def _reference_digest(block: bytes, block_size: int) -> int:
  mod = 1 << 16
  s1 = sum(block) % mod
  s2 = sum((block_size - idx + 1) * byte for idx, byte in enumerate(block, start=1)) % mod
  return (s2 << 16) | s1


def test_digest_matches_reference_implementation() -> None:
  block = bytes([1, 2, 3, 4])
  checksum = RollingChecksum(block, len(block))
  assert checksum.digest() == _reference_digest(block, len(block))


def test_roll_produces_same_digest_as_recomputation() -> None:
  data = bytes(range(1, 12))
  block_size = 4
  rolling = RollingChecksum(data[:block_size], block_size)
  expected_digest = rolling.digest()

  for start in range(1, len(data) - block_size + 1):
    out_byte = data[start - 1]
    in_byte = data[start + block_size - 1]
    rolling.roll(out_byte, in_byte)
    expected_digest = _reference_digest(data[start : start + block_size], block_size)
    assert rolling.digest() == expected_digest


def test_roll_handles_modulo_wraparound() -> None:
  block_size = 5
  # Construct data that forces the internal sums to overflow the 16-bit modulus.
  data = bytes([250, 251, 252, 253, 254, 255, 0])
  rolling = RollingChecksum(data[:block_size], block_size)
  rolling.roll(data[0], data[block_size])
  expected = RollingChecksum(data[1 : 1 + block_size], block_size).digest()
  assert rolling.digest() == expected
