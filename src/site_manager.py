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

from typing import Dict, List, Optional, Tuple

from dataclasses import dataclass
from .models import Variable, VariableVersion


@dataclass
class FailureRecord:
    """Failure window entry for a site."""
    fail_time: int
    recover_time: Optional[int] = None


class Site:
    """Single site / data manager."""

    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_up = True
        self.variables: Dict[str, Variable] = {}
        self.failure_history: List[FailureRecord] = []
        self._initialize_variables()

    def _initialize_variables(self):
        """Seed variables per distribution rules."""
        for i in range(1, 21):
            var_name = f"x{i}"
            if i % 2 == 0:
                self.variables[var_name] = Variable(
                    name=var_name,
                    index=i,
                    versions=[VariableVersion(value=10 * i, commit_time=0, transaction_id="init")],
                    is_readable=True
                )
            else:
                target_site = 1 + (i % 10)
                if self.site_id == target_site:
                    self.variables[var_name] = Variable(
                        name=var_name,
                        index=i,
                        versions=[VariableVersion(value=10 * i, commit_time=0, transaction_id="init")],
                        is_readable=True
                    )

    def has_variable(self, var_name: str) -> bool:
        return var_name in self.variables

    def fail(self, timestamp: int):
        """Mark the site offline."""
        # print(f"[debug] fail site {self.site_id} @ {timestamp}")
        self.is_up = False
        self.failure_history.append(FailureRecord(fail_time=timestamp))

    def recover(self, timestamp: int):
        """Bring the site back up and reset replicated readability."""
        # print(f"[debug] recover site {self.site_id} @ {timestamp}")
        self.is_up = True
        if self.failure_history:
            self.failure_history[-1].recover_time = timestamp

        for var_name, var in self.variables.items():
            var_index = int(var_name[1:])
            if var_index % 2 == 0:
                var.is_readable = False

    def was_up_continuously(self, from_time: int, to_time: int) -> bool:
        """True if no downtime overlaps [from_time, to_time)."""
        for record in self.failure_history:
            if record.fail_time > from_time and record.fail_time < to_time:
                return False
            if record.fail_time <= from_time:
                if record.recover_time is None or record.recover_time > from_time:
                    return False
        return True

    def get_last_failure_time(self) -> int:
        if not self.failure_history:
            return 0
        return self.failure_history[-1].fail_time

    def get_last_recovery_time(self) -> int:
        if not self.failure_history:
            return 0
        last_record = self.failure_history[-1]
        return last_record.recover_time if last_record.recover_time else 0

    def can_read_variable(self, var_name: str, txn_start_time: int) -> Tuple[bool, Optional[int]]:
        """Return readability + value snapshot for txn_start_time."""
        # print(f"[debug] read-check {var_name} at {txn_start_time} on site {self.site_id}")
        if not self.is_up:
            return False, None

        if var_name not in self.variables:
            return False, None

        var = self.variables[var_name]
        var_index = int(var_name[1:])

        if var_index % 2 == 1:
            value = var.get_value_at_time(txn_start_time)
            return value is not None, value

        last_recovery = self.get_last_recovery_time()
        txn_began_after_recovery = last_recovery > 0 and txn_start_time >= last_recovery

        if txn_began_after_recovery and not var.is_readable:
            return False, None

        for version in var.versions:
            if version.commit_time <= txn_start_time:
                if self.was_up_continuously(version.commit_time, txn_start_time):
                    return True, version.value
                break

        return False, None

    def write_variable(self, var_name: str, value: int, commit_time: int, txn_id: str):
        """Apply a committed value to local storage."""
        if var_name not in self.variables:
            return

        # print(f"[debug] commit {var_name}={value} on site {self.site_id} @ {commit_time}")
        var = self.variables[var_name]
        var.versions.insert(0, VariableVersion(
            value=value,
            commit_time=commit_time,
            transaction_id=txn_id
        ))
        var.is_readable = True

    def get_committed_value(self, var_name: str) -> Optional[int]:
        if var_name not in self.variables:
            return None
        return self.variables[var_name].get_latest_value()

    def dump(self) -> Dict[str, int]:
        result = {}
        for var_name in sorted(self.variables.keys(), key=lambda x: int(x[1:])):
            value = self.variables[var_name].get_latest_value()
            if value is not None:
                result[var_name] = value
        return result


class SiteManager:
    """Coordinator wrapper for the ten sites."""

    def __init__(self):
        self.sites: Dict[int, Site] = {}
        for i in range(1, 11):
            self.sites[i] = Site(i)

    def get_site(self, site_id: int) -> Optional[Site]:
        return self.sites.get(site_id)

    def fail_site(self, site_id: int, timestamp: int):
        if site_id in self.sites:
            self.sites[site_id].fail(timestamp)
            print(f"Site {site_id} failed")

    def recover_site(self, site_id: int, timestamp: int):
        if site_id in self.sites:
            self.sites[site_id].recover(timestamp)
            print(f"Site {site_id} recovered")

    def get_sites_for_variable(self, var_name: str) -> List[int]:
        var_index = int(var_name[1:])
        if var_index % 2 == 0:
            targets = list(range(1, 11))
        else:
            targets = [1 + (var_index % 10)]
        # print(f"[debug] var {var_name} lives at {targets}")
        return targets

    def get_up_sites_for_variable(self, var_name: str) -> List[int]:
        sites = self.get_sites_for_variable(var_name)
        return [s for s in sites if self.sites[s].is_up]

    def is_variable_replicated(self, var_name: str) -> bool:
        var_index = int(var_name[1:])
        return var_index % 2 == 0

    def dump_all(self):
        for site_id in sorted(self.sites.keys()):
            site = self.sites[site_id]
            values = site.dump()
            if values:
                value_str = ", ".join(f"{k}: {v}" for k, v in values.items())
                print(f"site {site_id} - {value_str}")
