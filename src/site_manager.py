"""
Site Manager for the Distributed Database System.

Authors: Shaoyi Zheng (sz3684) & Wenbo Lu (wl2707)
Date: December 5th 2025
Course: Advanced Database Systems

Description:
    Manages 10 database sites, each containing variables according to distribution rules:
    - Odd-indexed variables (x1, x3, ...): stored at site 1 + (index mod 10)
    - Even-indexed variables (x2, x4, ...): replicated across all 10 sites
    Each variable xi is initialized to value 10*i.

Classes:
    - FailureRecord: Records site failure/recovery events
    - Site: Represents a single database site (Data Manager)
    - SiteManager: Manages all 10 sites and routes operations

Key Functions:
    - Site.can_read_variable(): Check if variable can be read (Available Copies rules)
    - Site.was_up_continuously(): Check if site was up during a time interval
    - Site.write_variable(): Write a value at commit time
    - SiteManager.get_sites_for_variable(): Get sites containing a variable
    - SiteManager.dump_all(): Print all committed values at all sites

Side Effects:
    - Site failure/recovery modifies site state and variable readability
    - Writes modify variable version history
"""

from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from .models import Variable, VariableVersion


@dataclass
class FailureRecord:
    """Records a failure event at a site."""
    fail_time: int
    recover_time: Optional[int] = None


class Site:
    """
    Represents a single database site.

    Attributes:
        site_id: Site number (1-10)
        is_up: Whether the site is currently up
        variables: Dictionary of variables at this site
        failure_history: List of failure/recovery records
    """

    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_up = True
        self.variables: Dict[str, Variable] = {}
        self.failure_history: List[FailureRecord] = []
        self._initialize_variables()

    def _initialize_variables(self):
        """Initialize variables at this site based on distribution rules."""
        for i in range(1, 21):
            var_name = f"x{i}"
            if i % 2 == 0:
                # Even-indexed variables are at all sites
                self.variables[var_name] = Variable(
                    name=var_name,
                    index=i,
                    versions=[VariableVersion(value=10 * i, commit_time=0, transaction_id="init")],
                    is_readable=True
                )
            else:
                # Odd-indexed variables are at site 1 + (i mod 10)
                target_site = 1 + (i % 10)
                if self.site_id == target_site:
                    self.variables[var_name] = Variable(
                        name=var_name,
                        index=i,
                        versions=[VariableVersion(value=10 * i, commit_time=0, transaction_id="init")],
                        is_readable=True
                    )

    def has_variable(self, var_name: str) -> bool:
        """Check if this site has the given variable."""
        return var_name in self.variables

    def fail(self, timestamp: int):
        """Mark the site as failed."""
        self.is_up = False
        self.failure_history.append(FailureRecord(fail_time=timestamp))

    def recover(self, timestamp: int):
        """
        Recover the site.
        - Non-replicated variables are immediately available
        - Replicated variables need a committed write before being readable
        """
        self.is_up = True
        if self.failure_history:
            self.failure_history[-1].recover_time = timestamp

        # Mark replicated variables as not readable until a write commits
        for var_name, var in self.variables.items():
            var_index = int(var_name[1:])
            if var_index % 2 == 0:  # Replicated variable
                var.is_readable = False
            # Non-replicated variables remain readable

    def was_up_continuously(self, from_time: int, to_time: int) -> bool:
        """
        Check if the site was up continuously between two timestamps.
        Returns True if there was no failure during [from_time, to_time).
        """
        for record in self.failure_history:
            # Check if there was a failure that overlaps with the time range
            if record.fail_time > from_time and record.fail_time < to_time:
                return False
            # Check if site was down during this period
            if record.fail_time <= from_time:
                if record.recover_time is None or record.recover_time > from_time:
                    return False
        return True

    def get_last_failure_time(self) -> int:
        """Get the time of the last failure, or 0 if never failed."""
        if not self.failure_history:
            return 0
        return self.failure_history[-1].fail_time

    def get_last_recovery_time(self) -> int:
        """Get the time of the last recovery, or 0 if never recovered."""
        if not self.failure_history:
            return 0
        last_record = self.failure_history[-1]
        return last_record.recover_time if last_record.recover_time else 0

    def can_read_variable(self, var_name: str, txn_start_time: int) -> Tuple[bool, Optional[int]]:
        """
        Check if a variable can be read by a transaction that started at txn_start_time.
        Returns (can_read, value) tuple.

        For replicated variables:
        - The site must have been up continuously since the last commit before txn_start_time
        - If transaction began AFTER the site's last recovery, the variable must be readable
          (i.e., must have received a committed write since recovery)
        - If transaction began BEFORE the site's last failure, it can read even if
          the variable is currently marked unreadable

        For non-replicated variables:
        - Just needs to be up and have the variable
        """
        if not self.is_up:
            return False, None

        if var_name not in self.variables:
            return False, None

        var = self.variables[var_name]
        var_index = int(var_name[1:])

        # For non-replicated variables, just return the value at txn_start_time
        if var_index % 2 == 1:
            value = var.get_value_at_time(txn_start_time)
            return value is not None, value

        # For replicated variables:
        # Check if the transaction began after the last recovery
        last_recovery = self.get_last_recovery_time()
        txn_began_after_recovery = last_recovery > 0 and txn_start_time >= last_recovery

        # If transaction began after recovery, must check if variable is readable
        if txn_began_after_recovery and not var.is_readable:
            return False, None

        # Find the version that was committed before txn_start_time
        for version in var.versions:
            if version.commit_time <= txn_start_time:
                # Check if site was up continuously from commit to txn start
                if self.was_up_continuously(version.commit_time, txn_start_time):
                    return True, version.value
                break

        return False, None

    def write_variable(self, var_name: str, value: int, commit_time: int, txn_id: str):
        """
        Write a value to a variable (at commit time).
        Also marks replicated variables as readable after a committed write.
        """
        if var_name not in self.variables:
            return

        var = self.variables[var_name]
        var.versions.insert(0, VariableVersion(
            value=value,
            commit_time=commit_time,
            transaction_id=txn_id
        ))
        # After a committed write, replicated variables become readable
        var.is_readable = True

    def get_committed_value(self, var_name: str) -> Optional[int]:
        """Get the latest committed value of a variable."""
        if var_name not in self.variables:
            return None
        return self.variables[var_name].get_latest_value()

    def dump(self) -> Dict[str, int]:
        """Return all committed values at this site."""
        result = {}
        for var_name in sorted(self.variables.keys(), key=lambda x: int(x[1:])):
            value = self.variables[var_name].get_latest_value()
            if value is not None:
                result[var_name] = value
        return result


class SiteManager:
    """
    Manages all sites in the system.
    """

    def __init__(self):
        self.sites: Dict[int, Site] = {}
        for i in range(1, 11):
            self.sites[i] = Site(i)

    def get_site(self, site_id: int) -> Optional[Site]:
        """Get a site by ID."""
        return self.sites.get(site_id)

    def fail_site(self, site_id: int, timestamp: int):
        """Fail a site."""
        if site_id in self.sites:
            self.sites[site_id].fail(timestamp)
            print(f"Site {site_id} failed")

    def recover_site(self, site_id: int, timestamp: int):
        """Recover a site."""
        if site_id in self.sites:
            self.sites[site_id].recover(timestamp)
            print(f"Site {site_id} recovered")

    def get_sites_for_variable(self, var_name: str) -> List[int]:
        """Get all site IDs that have this variable."""
        var_index = int(var_name[1:])
        if var_index % 2 == 0:
            # Even-indexed: all sites
            return list(range(1, 11))
        else:
            # Odd-indexed: single site
            return [1 + (var_index % 10)]

    def get_up_sites_for_variable(self, var_name: str) -> List[int]:
        """Get all up site IDs that have this variable."""
        sites = self.get_sites_for_variable(var_name)
        return [s for s in sites if self.sites[s].is_up]

    def is_variable_replicated(self, var_name: str) -> bool:
        """Check if a variable is replicated."""
        var_index = int(var_name[1:])
        return var_index % 2 == 0

    def dump_all(self):
        """Dump all sites."""
        for site_id in sorted(self.sites.keys()):
            site = self.sites[site_id]
            values = site.dump()
            if values:
                value_str = ", ".join(f"{k}: {v}" for k, v in values.items())
                print(f"site {site_id} - {value_str}")
