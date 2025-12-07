"""
Transaction Manager for the Distributed Database System.

Authors: Shaoyi Zheng (sz3684) & Wenbo Lu (wl2707)
Date: December 5th 2025
Course: Advanced Database Systems

Description:
    Implements Serializable Snapshot Isolation (SSI) with the Available Copies
    algorithm. The Transaction Manager never fails and coordinates all transactions.

Key Algorithms:
    - Snapshot Isolation: Transactions read data as of their start time
    - First Committer Wins: WW conflicts resolved by commit order
    - Available Copies: Write to all up sites, read from any up site
    - SSI Cycle Detection: Detect dangerous cycles with consecutive RW edges

Key Functions:
    - begin_transaction(): Start a new transaction
    - read(): Execute snapshot read from available sites
    - write(): Buffer write to all available sites
    - end_transaction(): Validate and commit/abort transaction
    - _would_create_dangerous_cycle(): Check for SSI serialization failures
    - fail_site() / recover_site(): Handle site failure and recovery

Inputs:
    - Transaction IDs (e.g., "T1")
    - Variable names (e.g., "x1" to "x20")
    - Values for writes (integers)
    - Site IDs for failure/recovery (1-10)

Outputs:
    - Read values printed as "xN: value"
    - Commit/abort decisions printed as "TN commits" or "TN aborts"
    - Write locations printed as "TN writes xN=value to sites: ..."

Side Effects:
    - Modifies transaction states and histories
    - Applies writes to sites on commit
    - Maintains serialization graph edges for SSI validation
"""

from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from .models import Transaction, TransactionStatus, WaitingOperation
from .site_manager import SiteManager


class TransactionManager:
    """SSI + Available Copies brain."""

    def __init__(self):
        """
        Initialize the Transaction Manager.

        Purpose:
            Creates a new TransactionManager instance with all necessary data structures
            for managing transactions, sites, and serialization graph tracking.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            Initializes the site manager, transaction dictionary, and all tracking
            data structures (edges, commit history, snapshot reads).
        """
        self.site_manager = SiteManager()
        self.transactions: Dict[str, Transaction] = {}
        self.current_time = 0
        self.waiting_operations: List[WaitingOperation] = []
        # Track which transaction committed a write to each variable: var_name -> [(commit_time, txn_id)]
        self.variable_commit_history: Dict[str, List[Tuple[int, str]]] = defaultdict(list)

        # For SSI: track edges in the serialization graph
        # edges[from_txn][to_txn] = edge_type ('RW', 'WW', 'WR')
        self.edges: Dict[str, Dict[str, str]] = defaultdict(dict)

        # Track which variables each transaction read (for RW conflict detection)
        # And which transaction wrote each variable that was read from snapshot
        self.snapshot_reads: Dict[str, Dict[str, str]] = defaultdict(dict)  # tid -> {var: writer_tid}

    def tick(self):
        """
        Advance the global clock by one time unit.

        Purpose:
            Increments the current time counter, used for timestamp ordering
            and snapshot isolation.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            Modifies self.current_time by incrementing it by 1.
        """
        self.current_time += 1

    def begin_transaction(self, tid: str):
        """
        Start a new transaction.

        Purpose:
            Creates and registers a new transaction with the current timestamp
            as its start time for snapshot isolation.

        Inputs:
            tid (str): Transaction identifier (e.g., "T1", "T2")

        Outputs:
            None

        Side Effects:
            Creates a new Transaction object and adds it to self.transactions.
            Prints "{tid} begins" to stdout.
        """
        # print(f"[debug] begin {tid} @ {self.current_time}")
        txn = Transaction(
            tid=tid,
            start_time=self.current_time,
            status=TransactionStatus.ACTIVE
        )
        self.transactions[tid] = txn
        print(f"{tid} begins")

    def _get_snapshot_writer(self, var_name: str, txn_start_time: int) -> Optional[str]:
        """
        Get the transaction ID that wrote the variable version visible in the snapshot.

        Purpose:
            Determines which transaction committed the write to a variable that
            would be visible to a transaction starting at txn_start_time, for
            tracking read-write dependencies in SSI.

        Inputs:
            var_name (str): Name of the variable to check (e.g., "x1")
            txn_start_time (int): The start time of the transaction requesting the snapshot

        Outputs:
            Optional[str]: Transaction ID that wrote the visible version, or "init"
                          if no committed write exists before txn_start_time

        Side Effects:
            None
        """
        commits = self.variable_commit_history.get(var_name, [])
        latest_writer = "init"
        latest_time = 0
        for commit_time, writer_tid in commits:
            if commit_time <= txn_start_time and commit_time > latest_time:
                # import pdb; pdb.set_trace()
                latest_time = commit_time
                latest_writer = writer_tid
        return latest_writer

    def read(self, tid: str, var_name: str) -> Optional[int]:
        """
        Execute a snapshot read operation for a transaction.

        Purpose:
            Performs a read operation following snapshot isolation: reads from
            the transaction's write set if available, otherwise from its read set
            cache, or from an available site at the transaction's start time.
            Implements Available Copies algorithm by reading from any up site.

        Inputs:
            tid (str): Transaction identifier performing the read
            var_name (str): Name of the variable to read (e.g., "x1")

        Outputs:
            Optional[int]: The value read, or None if transaction is aborted,
                          waiting, or no site is available

        Side Effects:
            - Updates transaction's read_set and sites_accessed
            - Records snapshot writer for SSI conflict detection
            - May abort transaction if no valid data exists
            - May set transaction status to WAITING if no site is available
            - Adds waiting operation to queue if read cannot be satisfied
            - Prints "{var_name}: {value}" on successful read
            - Prints "{tid} waiting for {var_name} (no available site)" if waiting
            - Prints "{tid} aborts" if transaction is aborted
        """
        # print(f"[debug] {tid} read {var_name} @ {self.current_time}")
        if tid not in self.transactions:
            print(f"Error: Transaction {tid} not found")
            return None

        txn = self.transactions[tid]

        if txn.status == TransactionStatus.ABORTED:
            return None

        if var_name in txn.write_set:
            value, _ = txn.write_set[var_name]
            print(f"{var_name}: {value}")
            return value

        if var_name in txn.read_set:
            value = txn.read_set[var_name][0]
            print(f"{var_name}: {value}")
            return value

        sites = self.site_manager.get_sites_for_variable(var_name)
        is_replicated = self.site_manager.is_variable_replicated(var_name)

        readable_sites = []
        for site_id in sites:
            site = self.site_manager.get_site(site_id)
            if site and site.is_up:
                can_read, value = site.can_read_variable(var_name, txn.start_time)
                if can_read and value is not None:
                    readable_sites.append((site_id, value))

        if readable_sites:
            site_id, value = readable_sites[0]
            txn.read_set[var_name] = (value, site_id)
            txn.sites_accessed.add(site_id)

            if site_id not in txn.site_first_access_time:
                txn.site_first_access_time[site_id] = self.current_time

            writer = self._get_snapshot_writer(var_name, txn.start_time)
            self.snapshot_reads[tid][var_name] = writer

            self._check_rw_on_read(txn, var_name)

            print(f"{var_name}: {value}")
            return value

        if is_replicated:
            any_valid_site = False
            for site_id in sites:
                site = self.site_manager.get_site(site_id)
                if site and var_name in site.variables:
                    var = site.variables[var_name]
                    for version in var.versions:
                        if version.commit_time <= txn.start_time:
                            if site.was_up_continuously(version.commit_time, txn.start_time):
                                any_valid_site = True
                                break
                            break
                if any_valid_site:
                    break

            if not any_valid_site:
                self._abort_transaction(tid, f"No site has valid data for {var_name} - all sites failed after last commit")
                print(f"{tid} aborts")
                return None

        print(f"{tid} waiting for {var_name} (no available site)")
        txn.status = TransactionStatus.WAITING
        txn.waiting_for = var_name

        waiting_sites = set(sites)
        self.waiting_operations.append(WaitingOperation(
            transaction_id=tid,
            variable_name=var_name,
            required_sites=waiting_sites
        ))

        return None

    def _check_rw_on_read(self, reader_txn: Transaction, var_name: str):
        """
        Check for read-write conflicts when a transaction reads a variable.

        Purpose:
            Detects if any active transaction has written to the variable being read,
            and creates an RW edge in the serialization graph for SSI conflict detection.

        Inputs:
            reader_txn (Transaction): The transaction performing the read
            var_name (str): Name of the variable being read

        Outputs:
            None

        Side Effects:
            Adds RW edges to self.edges if conflicts are detected, where
            reader_txn -> other_txn indicates a read-write dependency.
        """
        for other_tid, other_txn in self.transactions.items():
            if other_tid == reader_txn.tid:
                continue
            if other_txn.status in (TransactionStatus.ABORTED, TransactionStatus.COMMITTED):
                continue

            if var_name in other_txn.write_set:
                self.edges[reader_txn.tid][other_tid] = 'RW'

    def write(self, tid: str, var_name: str, value: int):
        """
        Buffer a write operation for a transaction.

        Purpose:
            Records a write operation in the transaction's write set following
            the Available Copies algorithm: writes are buffered and will be
            applied to all up sites at commit time.

        Inputs:
            tid (str): Transaction identifier performing the write
            var_name (str): Name of the variable to write (e.g., "x1")
            value (int): The value to write

        Outputs:
            None

        Side Effects:
            - Updates transaction's write_set with the value and target sites
            - Updates transaction's sites_written and sites_accessed sets
            - Records site access and write times for failure detection
            - Checks for write dependencies and creates RW edges
            - Prints "{tid} writes {var_name}={value} to sites: ..." on success
            - Prints "{tid} writes {var_name}={value} (no sites available)" if no sites up
        """
        # print(f"[debug] {tid} write {var_name}={value} @ {self.current_time}")
        # import pdb; pdb.set_trace()
        if tid not in self.transactions:
            print(f"Error: Transaction {tid} not found")
            return

        txn = self.transactions[tid]

        if txn.status == TransactionStatus.ABORTED:
            return

        up_sites = set(self.site_manager.get_up_sites_for_variable(var_name))

        txn.write_set[var_name] = (value, up_sites)

        txn.sites_written.update(up_sites)
        txn.sites_accessed.update(up_sites)

        for site_id in up_sites:
            if site_id not in txn.site_first_access_time:
                txn.site_first_access_time[site_id] = self.current_time
            if site_id not in txn.site_write_time:
                txn.site_write_time[site_id] = self.current_time

        self._check_dependencies_on_write(txn, var_name)

        if up_sites:
            sites_str = ", ".join(str(s) for s in sorted(up_sites))
            print(f"{tid} writes {var_name}={value} to sites: {sites_str}")
        else:
            print(f"{tid} writes {var_name}={value} (no sites available)")

    def _check_dependencies_on_write(self, writer_txn: Transaction, var_name: str):
        """
        Check for read-write conflicts when a transaction writes a variable.

        Purpose:
            Detects if any active transaction has read the variable being written,
            and creates an RW edge in the serialization graph for SSI conflict detection.

        Inputs:
            writer_txn (Transaction): The transaction performing the write
            var_name (str): Name of the variable being written

        Outputs:
            None

        Side Effects:
            Adds RW edges to self.edges if conflicts are detected, where
            other_txn -> writer_txn indicates a read-write dependency.
        """
        for other_tid, other_txn in self.transactions.items():
            if other_tid == writer_txn.tid:
                continue
            if other_txn.status in (TransactionStatus.ABORTED, TransactionStatus.COMMITTED):
                continue

            if var_name in other_txn.read_set:
                self.edges[other_tid][writer_txn.tid] = 'RW'

            if var_name in other_txn.write_set:
                pass

    def end_transaction(self, tid: str):
        """
        End a transaction by validating and committing or aborting it.

        Purpose:
            Validates a transaction for commit by checking: (1) site failures after
            write, (2) first committer wins rule for write-write conflicts, and
            (3) SSI cycle detection for serialization conflicts. Commits if valid,
            aborts otherwise.

        Inputs:
            tid (str): Transaction identifier to end

        Outputs:
            None

        Side Effects:
            - May abort transaction if validation fails (site failure, first committer
              wins violation, or SSI cycle detected)
            - Commits transaction if all validations pass
            - Prints "{tid} commits" or "{tid} aborts" with reason
            - Updates transaction status and applies writes to sites on commit
        """
        # print(f"[debug] end {tid} @ {self.current_time}")
        if tid not in self.transactions:
            print(f"Error: Transaction {tid} not found")
            return

        txn = self.transactions[tid]

        if txn.status == TransactionStatus.ABORTED:
            print(f"{tid} aborts")
            return

        if txn.status == TransactionStatus.WAITING:
            print(f"{tid} aborts (still waiting)")
            self._abort_transaction(tid, "Transaction was waiting and ended")
            return

        for site_id in txn.sites_written:
            site = self.site_manager.get_site(site_id)
            if site:
                write_time = txn.site_write_time.get(site_id, txn.start_time)
                for record in site.failure_history:
                    if record.fail_time > write_time:
                        self._abort_transaction(tid, f"Site {site_id} failed after transaction wrote to it")
                        print(f"{tid} aborts")
                        return

        for var_name in txn.write_set:
            for commit_time, committed_tid in self.variable_commit_history.get(var_name, []):
                if commit_time > txn.start_time and committed_tid != tid:
                    self._abort_transaction(tid, f"First committer wins: {committed_tid} committed {var_name} first")
                    print(f"{tid} aborts")
                    return

        if self._would_create_dangerous_cycle(tid):
            self._abort_transaction(tid, "SSI cycle with consecutive RW edges detected")
            print(f"{tid} aborts")
            return

        self._commit_transaction(tid)
        print(f"{tid} commits")

    def _would_create_dangerous_cycle(self, tid: str) -> bool:
        """
        Check if committing this transaction would create a dangerous SSI cycle.

        Purpose:
            Detects if committing the transaction would create a cycle in the
            serialization graph with consecutive RW edges, which violates
            Serializable Snapshot Isolation (SSI) guarantees.

        Inputs:
            tid (str): Transaction identifier to check

        Outputs:
            bool: True if committing would create a dangerous cycle, False otherwise

        Side Effects:
            None
        """
        txn = self.transactions[tid]

        for var_name, (_, _) in txn.write_set.items():
            for _, committed_tid in self.variable_commit_history.get(var_name, []):
                if committed_tid == tid:
                    continue
                committed_txn = self.transactions.get(committed_tid)
                if not committed_txn or committed_txn.status != TransactionStatus.COMMITTED:
                    continue
                if self._can_reach_from_tid(tid, committed_tid, set()):
                    return True

        incoming_rw = []
        for other_tid, edges in self.edges.items():
            if tid in edges and edges[tid] == 'RW':
                other_txn = self.transactions.get(other_tid)
                if other_txn and other_txn.status == TransactionStatus.COMMITTED:
                    incoming_rw.append(other_tid)

        outgoing_rw = []
        if tid in self.edges:
            for target_tid, edge_type in self.edges[tid].items():
                if edge_type == 'RW':
                    target_txn = self.transactions.get(target_tid)
                    if target_txn and target_txn.status == TransactionStatus.COMMITTED:
                        outgoing_rw.append(target_tid)

        for in_txn in incoming_rw:
            for out_txn in outgoing_rw:
                if self._can_reach_via_committed(out_txn, in_txn, set()):
                    return True

        active_incoming_rw = []
        for other_tid, edges in self.edges.items():
            if tid in edges and edges[tid] == 'RW':
                other_txn = self.transactions.get(other_tid)
                if other_txn and other_txn.status == TransactionStatus.ACTIVE:
                    active_incoming_rw.append(other_tid)

        for in_txn in active_incoming_rw:
            for out_txn in outgoing_rw:
                if self._can_reach_via_committed(out_txn, in_txn, set()):
                    return True

        return False

    def _can_reach_from_tid(self, from_tid: str, to_tid: str, visited: Set[str]) -> bool:
        """
        Check if there is a path from one transaction to another in the serialization graph.

        Purpose:
            Performs a depth-first search to determine if a path exists from
            from_tid to to_tid in the serialization graph, used for cycle detection.

        Inputs:
            from_tid (str): Starting transaction identifier
            to_tid (str): Target transaction identifier
            visited (Set[str]): Set of already visited transaction IDs (for cycle prevention)

        Outputs:
            bool: True if a path exists from from_tid to to_tid, False otherwise

        Side Effects:
            Modifies the visited set by adding from_tid.
        """
        if from_tid == to_tid:
            return True
        if from_tid in visited:
            return False

        visited.add(from_tid)

        if from_tid in self.edges:
            for next_tid in self.edges[from_tid]:
                if self._can_reach_from_tid(next_tid, to_tid, visited):
                    return True

        return False

    def _can_reach_via_committed(self, from_tid: str, to_tid: str, visited: Set[str]) -> bool:
        """
        Check if a committed transaction can reach another transaction via edges or snapshot reads.

        Purpose:
            Determines if there is a path from a committed transaction (from_tid) to
            another transaction (to_tid) through either direct edges or snapshot read
            dependencies, used for SSI cycle detection.

        Inputs:
            from_tid (str): Starting transaction identifier (must be committed)
            to_tid (str): Target transaction identifier
            visited (Set[str]): Set of already visited transaction IDs (for cycle prevention)

        Outputs:
            bool: True if a path exists from from_tid to to_tid via committed edges
                  or snapshot reads, False otherwise

        Side Effects:
            Modifies the visited set by adding from_tid.
        """
        if from_tid == to_tid:
            return True
        if from_tid in visited:
            return False

        visited.add(from_tid)

        from_txn = self.transactions.get(from_tid)
        to_txn = self.transactions.get(to_tid)

        if not from_txn or not to_txn:
            return False

        if from_txn.status == TransactionStatus.COMMITTED:
            if from_tid in self.edges:
                for next_tid in self.edges[from_tid]:
                    if self._can_reach_via_committed(next_tid, to_tid, visited):
                        return True

            for var_name in to_txn.read_set:
                writer = self.snapshot_reads.get(to_tid, {}).get(var_name)
                if writer == from_tid:
                    return True

        return False

    def _commit_transaction(self, tid: str):
        """
        Commit a transaction by applying its writes to sites.

        Purpose:
            Applies all buffered writes in the transaction's write set to the
            currently available sites, records the commit in variable history,
            and marks the transaction as committed.

        Inputs:
            tid (str): Transaction identifier to commit

        Outputs:
            None

        Side Effects:
            - Writes variable values to sites that are currently up
            - Updates variable_commit_history with commit time and transaction ID
            - Changes transaction status to COMMITTED
            - Creates new variable versions on sites with commit timestamp
        """
        txn = self.transactions[tid]
        commit_time = self.current_time

        for var_name, (value, write_sites) in txn.write_set.items():
            current_up_sites = set(self.site_manager.get_up_sites_for_variable(var_name))
            sites_to_write = write_sites & current_up_sites

            for site_id in sites_to_write:
                site = self.site_manager.get_site(site_id)
                if site:
                    site.write_variable(var_name, value, commit_time, tid)
            # print(f"[debug] commit {tid} wrote {var_name} to {sorted(sites_to_write)} @ {commit_time}")

            self.variable_commit_history[var_name].append((commit_time, tid))

        txn.status = TransactionStatus.COMMITTED

    def _abort_transaction(self, tid: str, reason: str):
        """
        Abort a transaction and clean up its state.

        Purpose:
            Marks a transaction as aborted, removes it from waiting operations,
            and cleans up all edges in the serialization graph involving this
            transaction.

        Inputs:
            tid (str): Transaction identifier to abort
            reason (str): Reason for abort (for debugging/logging)

        Outputs:
            None

        Side Effects:
            - Changes transaction status to ABORTED
            - Records abort reason in transaction
            - Removes transaction from waiting_operations list
            - Removes all edges involving this transaction from the serialization graph
        """
        if tid not in self.transactions:
            return

        txn = self.transactions[tid]
        txn.status = TransactionStatus.ABORTED
        txn.abort_reason = reason
        # print(f"[debug] abort {tid}: {reason}")

        self.waiting_operations = [
            op for op in self.waiting_operations
            if op.transaction_id != tid
        ]

        if tid in self.edges:
            del self.edges[tid]
        for other_tid in list(self.edges.keys()):
            if tid in self.edges[other_tid]:
                del self.edges[other_tid][tid]

    def fail_site(self, site_id: int):
        """
        Mark a site as failed.

        Purpose:
            Records a site failure at the current time, which affects transaction
            validation and available copies algorithm.

        Inputs:
            site_id (int): Identifier of the site to fail (1-10)

        Outputs:
            None

        Side Effects:
            - Marks the site as down in the site manager
            - Records failure time in site's failure history
            - May cause transactions that wrote to this site to abort
        """
        self.site_manager.fail_site(site_id, self.current_time)

    def recover_site(self, site_id: int):
        """
        Recover a previously failed site.

        Purpose:
            Marks a site as up again and processes any waiting operations that
            may now be able to proceed.

        Inputs:
            site_id (int): Identifier of the site to recover (1-10)

        Outputs:
            None

        Side Effects:
            - Marks the site as up in the site manager
            - Records recovery time in site's failure history
            - Processes waiting operations to see if they can now complete
            - May unblock transactions waiting for this site
        """
        self.site_manager.recover_site(site_id, self.current_time)
        self._process_waiting_operations()

    def _process_waiting_operations(self):
        """
        Attempt to satisfy waiting read operations after site recovery.

        Purpose:
            Re-evaluates all waiting operations to see if they can now be
            satisfied after a site has recovered, and reactivates transactions
            that successfully read.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            - Attempts to read variables for waiting transactions
            - Changes transaction status from WAITING to ACTIVE if read succeeds
            - Updates waiting_operations list to remove satisfied operations
            - May print read values if reads succeed
        """
        still_waiting = []

        for op in self.waiting_operations:
            txn = self.transactions.get(op.transaction_id)
            if not txn or txn.status != TransactionStatus.WAITING:
                continue

            result = self._try_read(op.transaction_id, op.variable_name)
            if result is not None:
                txn.status = TransactionStatus.ACTIVE
                txn.waiting_for = None
            else:
                still_waiting.append(op)

        self.waiting_operations = still_waiting

    def _try_read(self, tid: str, var_name: str) -> Optional[int]:
        """
        Attempt to read a variable for a waiting transaction.

        Purpose:
            Tries to read a variable from any available site for a transaction
            that was previously waiting, used when sites recover.

        Inputs:
            tid (str): Transaction identifier attempting the read
            var_name (str): Name of the variable to read

        Outputs:
            Optional[int]: The value read if successful, None if no site is available

        Side Effects:
            - Updates transaction's read_set and sites_accessed if read succeeds
            - Records snapshot writer for SSI conflict detection
            - Checks for RW conflicts and creates edges
            - Prints "{var_name}: {value}" if read succeeds
        """
        txn = self.transactions[tid]
        sites = self.site_manager.get_sites_for_variable(var_name)

        for site_id in sites:
            site = self.site_manager.get_site(site_id)
            if site and site.is_up:
                can_read, value = site.can_read_variable(var_name, txn.start_time)
                if can_read and value is not None:
                    txn.read_set[var_name] = (value, site_id)
                    txn.sites_accessed.add(site_id)

                    writer = self._get_snapshot_writer(var_name, txn.start_time)
                    self.snapshot_reads[tid][var_name] = writer
                    self._check_rw_on_read(txn, var_name)

                    print(f"{var_name}: {value}")
                    return value

        return None

    def dump(self):
        """
        Print the current state of all sites and their variables.

        Purpose:
            Outputs a dump of all site states, variable values, and versions
            for debugging and verification purposes.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            Prints site and variable information to stdout via site_manager.
        """
        self.site_manager.dump_all()

    def query_state(self):
        """
        Print detailed system state for debugging.

        Purpose:
            Outputs comprehensive information about the current system state
            including time, site statuses, transaction states, serialization
            graph edges, and waiting operations.

        Inputs:
            None

        Outputs:
            None

        Side Effects:
            Prints formatted system state information to stdout including:
            - Current time
            - Site up/down status
            - Transaction statuses, read sets, and write sets
            - Serialization graph edges
            - Waiting operations
        """
        print("\n=== System State ===")
        print(f"Current time: {self.current_time}")

        print("\n--- Sites ---")
        for site_id, site in self.site_manager.sites.items():
            status = "UP" if site.is_up else "DOWN"
            print(f"Site {site_id}: {status}")

        print("\n--- Transactions ---")
        for tid, txn in self.transactions.items():
            print(f"{tid}: status={txn.status.value}, start={txn.start_time}")
            if txn.read_set:
                print(f"  reads: {txn.read_set}")
            if txn.write_set:
                print(f"  writes: {txn.write_set}")

        print("\n--- Edges ---")
        for from_tid, targets in self.edges.items():
            for to_tid, edge_type in targets.items():
                print(f"  {from_tid} --{edge_type}--> {to_tid}")

        print("\n--- Waiting Operations ---")
        for op in self.waiting_operations:
            print(f"{op.transaction_id} waiting for {op.variable_name}")

        print("===================\n")
