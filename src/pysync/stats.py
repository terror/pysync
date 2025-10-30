from dataclasses import dataclass


@dataclass(frozen=True)
class SyncStats:
  total_bytes: int
  bytes_transferred: int
  bytes_reused: int

  @property
  def bytes_saved(self) -> int:
    return max(self.total_bytes - self.bytes_transferred, 0)
