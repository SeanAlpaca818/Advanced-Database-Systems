#!/usr/bin/env python3
"""
Main entry point for the Distributed Database System.
Replicated Concurrency Control and Recovery using SSI and Available Copies.

Authors: Shaoyi Zheng (sz3684) & Wenbo Lu (wl2707)
Date: December 5th 2025
Course: Advanced Database Systems

Description:
    Entry point that reads commands from a file or stdin and executes them
    through the Transaction Manager. Each line advances the logical clock by 1.

Inputs:
    - Command file path (optional, reads from stdin if not provided)
    - Supported commands: begin, R, W, end, fail, recover, dump, querystate

Outputs:
    - Transaction results (commits/aborts)
    - Read values
    - Site status changes
    - System state dumps

Side Effects:
    - Modifies transaction and site states in the Transaction Manager
"""

import sys
from src.parser import Parser, CommandType
from src.transaction_manager import TransactionManager


def run_from_file(filename: str):
    """Run the database system from an input file."""
    tm = TransactionManager()

    with open(filename, 'r') as f:
        for line in f:
            process_line(tm, line)


def run_interactive():
    """Run the database system interactively from stdin."""
    tm = TransactionManager()
    print("Distributed Database System - Interactive Mode")
    print("Enter commands (Ctrl+D to exit):")

    try:
        for line in sys.stdin:
            process_line(tm, line)
    except EOFError:
        pass


def process_line(tm: TransactionManager, line: str):
    """Process a single line of input."""
    cmd = Parser.parse_line(line)

    # Each line advances time
    tm.tick()

    if cmd.cmd_type == CommandType.EMPTY:
        return

    if cmd.cmd_type == CommandType.COMMENT:
        return

    if cmd.cmd_type == CommandType.BEGIN:
        tm.begin_transaction(cmd.transaction_id)

    elif cmd.cmd_type == CommandType.READ:
        tm.read(cmd.transaction_id, cmd.variable)

    elif cmd.cmd_type == CommandType.WRITE:
        tm.write(cmd.transaction_id, cmd.variable, cmd.value)

    elif cmd.cmd_type == CommandType.END:
        tm.end_transaction(cmd.transaction_id)

    elif cmd.cmd_type == CommandType.FAIL:
        tm.fail_site(cmd.site_id)

    elif cmd.cmd_type == CommandType.RECOVER:
        tm.recover_site(cmd.site_id)

    elif cmd.cmd_type == CommandType.DUMP:
        tm.dump()

    elif cmd.cmd_type == CommandType.QUERY_STATE:
        tm.query_state()


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        run_from_file(filename)
    else:
        run_interactive()


if __name__ == "__main__":
    main()
