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
        """
        Initialize a new Site instance.

        Purpose:
            Creates a new database site with the given ID, initializes all
            variables according to distribution rules, and sets initial state.

        Inputs:
            site_id (int): Unique identifier for this site (1-10)

        Outputs:
            None

        Side Effects:
            - Initializes site state (is_up=True, empty failure history)
            - Creates and initializes variables based on distribution rules
            - Calls _initialize_variables() to populate variable storage
        """
        self.site_id = site_id
        self.is_up = True
        self.variables: Dict[str, Variable] = {}
        self.failure_history: List[FailureRecord] = []
        self._initialize_variables()

    def _initialize_variables(self):
        """
        Initialize all variables according to distribution rules.

        Purpose:
            Creates and initializes variables x1 through x20 following the
            distribution rules: odd-indexed variables go to specific sites,
            even-indexed variables are replicated to all sites.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            - Creates Variable objects in self.variables dictionary
            - Initializes each variable with value 10*i at commit_time=0
            - Sets is_readable=True for all variables initially
        """
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
        """
        Check if this site stores the given variable.

        Purpose:
            Determines whether the site contains the specified variable
            in its local storage.

        Inputs:
            var_name (str): Name of the variable to check (e.g., "x1", "x2")

        Outputs:
            bool: True if the site stores this variable, False otherwise

        Side Effects:
            None
        """
        return var_name in self.variables

    def fail(self, timestamp: int):
        """
        Mark the site as failed/offline.

        Purpose:
            Records a site failure event at the given timestamp, marking
            the site as down and adding a failure record to history.

        Inputs:
            timestamp (int): The time when the site failed

        Outputs:
            None

        Side Effects:
            - Sets self.is_up to False
            - Adds a FailureRecord to failure_history with the fail_time
            - Affects variable readability for replicated variables
        """
        # print(f"[debug] fail site {self.site_id} @ {timestamp}")
        self.is_up = False
        self.failure_history.append(FailureRecord(fail_time=timestamp))

    def recover(self, timestamp: int):
        """
        Recover the site and reset replicated variable readability.

        Purpose:
            Marks the site as up again, records the recovery time, and
            sets all replicated (even-indexed) variables to non-readable
            until they are written to again (Available Copies rule).

        Inputs:
            timestamp (int): The time when the site recovered

        Outputs:
            None

        Side Effects:
            - Sets self.is_up to True
            - Updates the most recent FailureRecord with recover_time
            - Sets is_readable=False for all even-indexed (replicated) variables
        """
        # print(f"[debug] recover site {self.site_id} @ {timestamp}")
        self.is_up = True
        if self.failure_history:
            self.failure_history[-1].recover_time = timestamp

        for var_name, var in self.variables.items():
            var_index = int(var_name[1:])
            if var_index % 2 == 0:
                var.is_readable = False

    def was_up_continuously(self, from_time: int, to_time: int) -> bool:
        """
        Check if the site was continuously up during a time interval.

        Purpose:
            Determines whether the site was up and operational throughout
            the entire interval [from_time, to_time), with no failures
            overlapping this period.

        Inputs:
            from_time (int): Start of the time interval (inclusive)
            to_time (int): End of the time interval (exclusive)

        Outputs:
            bool: True if site was up continuously during the interval,
                 False if any failure overlapped the interval

        Side Effects:
            None
        """
        for record in self.failure_history:
            if record.fail_time > from_time and record.fail_time < to_time:
                return False
            if record.fail_time <= from_time:
                if record.recover_time is None or record.recover_time > from_time:
                    return False
        return True

    def get_last_failure_time(self) -> int:
        """
        Get the timestamp of the most recent site failure.

        Purpose:
            Returns the fail_time of the most recent failure record,
            used for tracking site availability history.

        Inputs:
            None

        Outputs:
            int: The timestamp of the last failure, or 0 if no failures occurred

        Side Effects:
            None
        """
        if not self.failure_history:
            return 0
        return self.failure_history[-1].fail_time

    def get_last_recovery_time(self) -> int:
        """
        Get the timestamp of the most recent site recovery.

        Purpose:
            Returns the recover_time of the most recent failure record,
            used for determining if a transaction started after recovery
            (affects replicated variable readability).

        Inputs:
            None

        Outputs:
            int: The timestamp of the last recovery, or 0 if no recovery
                 occurred or site is still down

        Side Effects:
            None
        """
        if not self.failure_history:
            return 0
        last_record = self.failure_history[-1]
        return last_record.recover_time if last_record.recover_time else 0

    def can_read_variable(self, var_name: str, txn_start_time: int) -> Tuple[bool, Optional[int]]:
        """
        Check if a variable can be read and return its snapshot value.

        Purpose:
            Determines if a variable is readable at a given transaction start time
            following Available Copies rules: for replicated variables, the site
            must have been up continuously since the version was committed, and
            if the transaction started after recovery, the variable must be readable.

        Inputs:
            var_name (str): Name of the variable to read (e.g., "x1", "x2")
            txn_start_time (int): The start time of the transaction requesting the read

        Outputs:
            Tuple[bool, Optional[int]]: A tuple containing:
                - bool: True if the variable can be read, False otherwise
                - Optional[int]: The value at txn_start_time if readable, None otherwise

        Side Effects:
            None
        """
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
        """
        Apply a committed write to local variable storage.

        Purpose:
            Creates a new version of the variable with the committed value,
            adding it to the version history and marking replicated variables
            as readable (Available Copies rule: write makes variable readable).

        Inputs:
            var_name (str): Name of the variable to write (e.g., "x1", "x2")
            value (int): The value to write
            commit_time (int): The timestamp when the transaction committed
            txn_id (str): The identifier of the committing transaction

        Outputs:
            None

        Side Effects:
            - Inserts a new VariableVersion at the front of the versions list
            - Sets is_readable=True for the variable (important for replicated variables)
        """
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
        """
        Get the latest committed value of a variable.

        Purpose:
            Returns the most recent committed value of the variable,
            which is the value from the latest version in the version history.

        Inputs:
            var_name (str): Name of the variable to query (e.g., "x1", "x2")

        Outputs:
            Optional[int]: The latest committed value, or None if the variable
                          doesn't exist or has no versions

        Side Effects:
            None
        """
        if var_name not in self.variables:
            return None
        return self.variables[var_name].get_latest_value()

    def dump(self) -> Dict[str, int]:
        """
        Get a dictionary of all variable names and their latest committed values.

        Purpose:
            Returns a snapshot of all variables stored at this site with their
            current committed values, sorted by variable index.

        Inputs:
            None

        Outputs:
            Dict[str, int]: Dictionary mapping variable names to their latest
                          committed values (only includes variables with values)

        Side Effects:
            None
        """
        result = {}
        for var_name in sorted(self.variables.keys(), key=lambda x: int(x[1:])):
            value = self.variables[var_name].get_latest_value()
            if value is not None:
                result[var_name] = value
        return result


class SiteManager:
    """Coordinator wrapper for the ten sites."""

    def __init__(self):
        """
        Initialize the Site Manager with 10 database sites.

        Purpose:
            Creates and initializes all 10 database sites (sites 1-10),
            each with their variables according to distribution rules.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            - Creates 10 Site instances and stores them in self.sites dictionary
            - Each site initializes its variables via Site.__init__
        """
        self.sites: Dict[int, Site] = {}
        for i in range(1, 11):
            self.sites[i] = Site(i)

    def get_site(self, site_id: int) -> Optional[Site]:
        """
        Get a Site instance by its ID.

        Purpose:
            Retrieves the Site object for the given site identifier.

        Inputs:
            site_id (int): The identifier of the site to retrieve (1-10)

        Outputs:
            Optional[Site]: The Site object if it exists, None otherwise

        Side Effects:
            None
        """
        return self.sites.get(site_id)

    def fail_site(self, site_id: int, timestamp: int):
        """
        Mark a site as failed.

        Purpose:
            Records a failure event for the specified site at the given
            timestamp, marking it as down.

        Inputs:
            site_id (int): The identifier of the site to fail (1-10)
            timestamp (int): The time when the site failed

        Outputs:
            None

        Side Effects:
            - Calls Site.fail() on the specified site
            - Prints "Site {site_id} failed" to stdout
        """
        if site_id in self.sites:
            self.sites[site_id].fail(timestamp)
            print(f"Site {site_id} failed")

    def recover_site(self, site_id: int, timestamp: int):
        """
        Recover a previously failed site.

        Purpose:
            Marks the specified site as up again and resets replicated
            variable readability according to Available Copies rules.

        Inputs:
            site_id (int): The identifier of the site to recover (1-10)
            timestamp (int): The time when the site recovered

        Outputs:
            None

        Side Effects:
            - Calls Site.recover() on the specified site
            - Prints "Site {site_id} recovered" to stdout
            - Resets is_readable=False for replicated variables on the site
        """
        if site_id in self.sites:
            self.sites[site_id].recover(timestamp)
            print(f"Site {site_id} recovered")

    def get_sites_for_variable(self, var_name: str) -> List[int]:
        """
        Get the list of site IDs that store a given variable.

        Purpose:
            Returns all sites that contain the variable according to
            distribution rules: even-indexed variables are replicated
            to all 10 sites, odd-indexed variables go to one specific site.

        Inputs:
            var_name (str): Name of the variable (e.g., "x1", "x2")

        Outputs:
            List[int]: List of site IDs (1-10) that store this variable

        Side Effects:
            None
        """
        var_index = int(var_name[1:])
        if var_index % 2 == 0:
            targets = list(range(1, 11))
        else:
            targets = [1 + (var_index % 10)]
        # print(f"[debug] var {var_name} lives at {targets}")
        return targets

    def get_up_sites_for_variable(self, var_name: str) -> List[int]:
        """
        Get the list of currently up sites that store a given variable.

        Purpose:
            Returns only the sites that both store the variable AND are
            currently operational, used by Available Copies algorithm
            to determine where writes can be applied.

        Inputs:
            var_name (str): Name of the variable (e.g., "x1", "x2")

        Outputs:
            List[int]: List of site IDs (1-10) that store this variable
                      and are currently up

        Side Effects:
            None
        """
        sites = self.get_sites_for_variable(var_name)
        return [s for s in sites if self.sites[s].is_up]

    def is_variable_replicated(self, var_name: str) -> bool:
        """
        Check if a variable is replicated across all sites.

        Purpose:
            Determines whether a variable follows the replication rule:
            even-indexed variables are replicated, odd-indexed are not.

        Inputs:
            var_name (str): Name of the variable to check (e.g., "x1", "x2")

        Outputs:
            bool: True if the variable is replicated (even index), False otherwise

        Side Effects:
            None
        """
        var_index = int(var_name[1:])
        return var_index % 2 == 0

    def dump_all(self):
        """
        Print the current state of all sites and their variables.

        Purpose:
            Outputs a formatted dump of all sites showing their variable
            names and latest committed values, used for debugging and
            verification of system state.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            Prints formatted output to stdout showing each site's variables
            in the format "site {id} - x1: value1, x2: value2, ..."
        """
        for site_id in sorted(self.sites.keys()):
            site = self.sites[site_id]
            values = site.dump()
            if values:
                value_str = ", ".join(f"{k}: {v}" for k, v in values.items())
                print(f"site {site_id} - {value_str}")
