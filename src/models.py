"""
Data Models for the Distributed Database System.

Authors: Shaoyi Zheng (sz3684) & Wenbo Lu (wl2707)
Date: December 5th 2025
Course: Advanced Database Systems

Description:
    Defines core data structures used throughout the system including
    transaction status, variable versions, and transaction metadata.

Classes:
    - TransactionStatus: Enum for transaction states (ACTIVE, COMMITTED, ABORTED, WAITING)
    - VariableVersion: Represents a single version of a variable with value and commit time
    - Variable: Represents a variable with multi-version history
    - Transaction: Represents a transaction with read/write sets and metadata
    - WaitingOperation: Represents a pending read operation waiting for site recovery

Side Effects:
    None - pure data structures
"""

from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field


class TransactionStatus(Enum):
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"
    WAITING = "waiting"


@dataclass
class VariableVersion:
    value: int
    commit_time: int
    transaction_id: str


@dataclass
class Variable:
    name: str
    index: int
    versions: List[VariableVersion] = field(default_factory=list)
    is_readable: bool = True

    def get_value_at_time(self, timestamp: int) -> Optional[int]:
        """Return committed value <= timestamp."""
        for version in self.versions:
            if version.commit_time <= timestamp:
                return version.value
        return None

    def get_latest_value(self) -> Optional[int]:
        if self.versions:
            return self.versions[0].value
        return None

    def get_latest_commit_time(self) -> int:
        if self.versions:
            return self.versions[0].commit_time
        return 0


@dataclass
class Transaction:
    tid: str
    start_time: int
    status: TransactionStatus = TransactionStatus.ACTIVE
    read_set: Dict[str, Tuple[int, int]] = field(default_factory=dict)  # var_name -> (value, site_id)
    write_set: Dict[str, Tuple[int, Set[int]]] = field(default_factory=dict)  # var_name -> (value, sites_at_write_time)
    sites_accessed: Set[int] = field(default_factory=set)
    sites_written: Set[int] = field(default_factory=set)
    site_first_access_time: Dict[int, int] = field(default_factory=dict)  # site_id -> first access timestamp
    site_write_time: Dict[int, int] = field(default_factory=dict)  # site_id -> first write timestamp (for Available Copies)
    abort_reason: Optional[str] = None
    waiting_for: Optional[str] = None  # Variable waiting for


@dataclass
class WaitingOperation:
    transaction_id: str
    variable_name: str
    required_sites: Set[int]  # Sites that could satisfy this read
