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
        self.current_time += 1

    def begin_transaction(self, tid: str):
        # print(f"[debug] begin {tid} @ {self.current_time}")
        txn = Transaction(
            tid=tid,
            start_time=self.current_time,
            status=TransactionStatus.ACTIVE
        )
        self.transactions[tid] = txn
        print(f"{tid} begins")

    def _get_snapshot_writer(self, var_name: str, txn_start_time: int) -> Optional[str]:
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
        for other_tid, other_txn in self.transactions.items():
            if other_tid == reader_txn.tid:
                continue
            if other_txn.status in (TransactionStatus.ABORTED, TransactionStatus.COMMITTED):
                continue

            if var_name in other_txn.write_set:
                self.edges[reader_txn.tid][other_tid] = 'RW'

    def write(self, tid: str, var_name: str, value: int):
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
        self.site_manager.fail_site(site_id, self.current_time)

    def recover_site(self, site_id: int):
        self.site_manager.recover_site(site_id, self.current_time)
        self._process_waiting_operations()

    def _process_waiting_operations(self):
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
        self.site_manager.dump_all()

    def query_state(self):
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
