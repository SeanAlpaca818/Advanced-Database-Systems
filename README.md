# Distributed Database System with Serializable Snapshot Isolation

This is a distributed database system implementing SSI (Serializable Snapshot Isolation) and the Available Copies algorithm.

## Quick Start

```bash
# Run a single test
python3 main.py tests/test1.txt

# Run all tests
python3 run_tests.py
```

## Project Structure

```
Final_Proj/
├── main.py              # Entry point, parses and executes input files
├── run_tests.py         # Test runner, executes all test cases
├── src/
│   ├── __init__.py      # Package initialization file
│   ├── models.py        # Data model definitions
│   ├── parser.py        # Command parser
│   ├── site_manager.py  # Site manager (data management)
│   └── transaction_manager.py  # Transaction manager (SSI core logic)
└── tests/
    ├── test1.txt        # Test cases 1-25
    ├── test2.txt
    └── ...
```

## Core Modules (Execution Order)

### 1. `src/parser.py` - Command Parser

Parses commands from input files:

| Command Format | Description |
|----------------|-------------|
| `begin(Ti)` | Begin transaction Ti |
| `R(Ti, xj)` | Transaction Ti reads variable xj |
| `W(Ti, xj, v)` | Transaction Ti writes value v to variable xj |
| `end(Ti)` | End transaction Ti |
| `fail(k)` | Site k fails |
| `recover(k)` | Site k recovers |
| `dump()` | Print all site states |

### 2. `src/transaction_manager.py` - Transaction Manager

Implements the core SSI logic:

- **Read Operation (`read`)**:
  - Reads snapshot at transaction start time from available sites
  - If all sites are unavailable, transaction enters waiting state

- **Write Operation (`write`)**:
  - Writes to all currently available sites
  - Records which sites were available at write time

- **Commit Detection (`end_transaction`)**:
  1. **Available Copies Check**: Whether sites accessed by transaction failed after access
  2. **First Committer Wins**: Check for WW conflicts
  3. **Dangerous Cycle Detection**: Check for cycles formed by consecutive RW edges

- **Cycle Detection (`_would_create_dangerous_cycle`)**:
  - Check RW edges: T1 read a variable that T2 later wrote
  - Check WW edges: T1 and committed transaction wrote the same variable
  - Detect cycles through `_can_reach_from_tid()`

### 3. `src/site_manager.py` - Site Manager

Responsible for data management at a single site:

- **Variable Distribution Rules**:
  - Odd-indexed variables (x1, x3, ...) → Only at site `1 + (index mod 10)`
  - Even-indexed variables (x2, x4, ...) → Replicated to all 10 sites
  - Every `xi` starts at value `10 * i`

- **Main Functions**:
  - `get_sites_for_variable()` / `get_up_sites_for_variable()` - Resolve placement and currently up replicas
  - `fail()` / `recover()` - Track failures and reset readability state after recovery
  - `can_read_variable()` - Enforce snapshot rules per site (including continuous uptime checks)
  - `was_up_continuously()` - Helper used by `can_read_variable`
  - `write_variable()` - Append a committed version and, for replicated vars, mark them readable
  - `dump_all()` - Emit per-site committed values

### 4. `src/models.py` - Data Models

Defines the core data structures of the system:

| Class Name | Description |
|------------|-------------|
| `TransactionStatus` | Transaction status enum (ACTIVE, COMMITTED, ABORTED, WAITING) |
| `VariableVersion` | Variable version, contains value, commit time, transaction ID |
| `Variable` | Variable, maintains multi-version history |
| `Transaction` | Transaction, contains read set, write set, accessed sites, etc. |
| `WaitingOperation` | Pending read operation |

## Core Algorithms

### Serializable Snapshot Isolation (SSI)

1. **Snapshot Read**: Each transaction reads the database snapshot at its start time
2. **First Committer Wins**: If two concurrent transactions write the same variable, the first to commit wins
3. **Dangerous Structure Detection**: Detect cycles T1 → T2 → T3 → T1 that contain consecutive RW edges

### Available Copies Algorithm

1. **Read**: Read from any available site (for replicated variables)
2. **Write**: Write to all available sites
3. **Readability After Recovery**: Replicated variables need to be written after site recovery to be readable
4. **Failure Detection**: If a site accessed by a transaction fails after access, the transaction must abort

## Test Case Coverage

| Test Type | Test Numbers |
|-----------|--------------|
| First Committer Wins | test1, test13, test14, test15, test20 |
| Snapshot Isolation | test2, test7, test8, test9, test10 |
| Site Failure/Recovery | test3, test3_5, test3_7, test4, test5, test6, test17, test19 |
| RW/WW Cycle Detection | test18, test21, test22 |
| All Sites Failure | test23, test24 |
| Transaction Waiting | test25 |
