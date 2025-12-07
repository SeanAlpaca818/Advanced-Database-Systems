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
    """Status of a transaction."""
    ACTIVE = "active"
    COMMITTED = "committed"
    ABORTED = "aborted"
    WAITING = "waiting"


@dataclass
class VariableVersion:
    """
    Represents a version of a variable.

    Attributes:
        value: The value of this version
        commit_time: The timestamp when this version was committed
        transaction_id: The transaction that wrote this version
    """
    value: int
    commit_time: int
    transaction_id: str


@dataclass
class Variable:
    """
    Represents a variable at a site.

    Attributes:
        name: Variable name (e.g., "x1")
        index: Variable index (1-20)
        versions: List of committed versions, newest first
        is_readable: Whether the variable can be read (for replicated vars after recovery)
    """
    name: str
    index: int
    versions: List[VariableVersion] = field(default_factory=list)
    is_readable: bool = True

    def get_value_at_time(self, timestamp: int) -> Optional[int]:
        """Get the committed value as of the given timestamp."""
        for version in self.versions:
            if version.commit_time <= timestamp:
                return version.value
        return None

    def get_latest_value(self) -> Optional[int]:
        """Get the most recent committed value."""
        if self.versions:
            return self.versions[0].value
        return None

    def get_latest_commit_time(self) -> int:
        """Get the commit time of the latest version."""
        if self.versions:
            return self.versions[0].commit_time
        return 0


@dataclass
class Transaction:
    """
    Represents a transaction.

    Attributes:
        tid: Transaction ID (e.g., "T1")
        start_time: The timestamp when the transaction began
        status: Current status of the transaction
        read_set: Variables read by this transaction: {var_name: (value, site_id)}
        write_set: Variables written by this transaction: {var_name: (value, sites)}
        sites_accessed: Sites that this transaction has accessed (for available copies)
        sites_written: Sites that this transaction has written to
        site_first_access_time: When each site was first accessed: {site_id: timestamp}
        site_write_time: When each site was first written to: {site_id: timestamp} (for Available Copies abort rule)
        abort_reason: Reason for abort if aborted
    """
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
    """Represents a waiting read operation."""
    transaction_id: str
    variable_name: str
    required_sites: Set[int]  # Sites that could satisfy this read
