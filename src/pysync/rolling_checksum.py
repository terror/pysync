class RollingChecksum:
  """
  Implements the rolling checksum described in the rsync algorithm.

  For additional reading on the algorithm, check out https://rsync.samba.org/tech_report/node3.html.
  """

  _MOD = 1 << 16

  def __init__(self, block: bytes, block_size: int):
    self.block_size = block_size
    self.s1 = sum(block) % self._MOD
    self.s2 = (
      sum((self.block_size - idx + 1) * byte for idx, byte in enumerate(block, start=1)) % self._MOD
    )

  def digest(self) -> int:
    return (self.s2 << 16) | self.s1

  def roll(self, out_byte: int, in_byte: int) -> None:
    self.s1 = (self.s1 - out_byte + in_byte) % self._MOD
    self.s2 = (self.s2 - self.block_size * out_byte + self.s1) % self._MOD
