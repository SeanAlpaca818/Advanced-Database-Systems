"""
Command Parser for the Distributed Database System.

Authors: Shaoyi Zheng (sz3684) & Wenbo Lu (wl2707)
Date: December 5th 2025
Course: Advanced Database Systems

Description:
    Parses input commands from files or stdin using regular expressions.
    Supports the standard command format specified in the project requirements.

Supported Commands:
    - begin(Ti): Begin transaction Ti
    - R(Ti, xj): Transaction Ti reads variable xj
    - W(Ti, xj, v): Transaction Ti writes value v to variable xj
    - end(Ti): End transaction Ti (commit or abort)
    - fail(k): Site k fails
    - recover(k): Site k recovers
    - dump(): Print all committed values at all sites
    - querystate(): Print system state for debugging

Classes:
    - CommandType: Enum for command types
    - Command: Dataclass representing a parsed command
    - Parser: Static methods for parsing input

Inputs:
    - Raw text lines from input file or stdin

Outputs:
    - Command objects with parsed parameters

Side Effects:
    None - pure parsing functions
"""

import re
from typing import Optional, List
from dataclasses import dataclass
from enum import Enum


class CommandType(Enum):
    BEGIN = "begin"
    READ = "read"
    WRITE = "write"
    END = "end"
    FAIL = "fail"
    RECOVER = "recover"
    DUMP = "dump"
    QUERY_STATE = "querystate"
    COMMENT = "comment"
    EMPTY = "empty"


@dataclass
class Command:
    cmd_type: CommandType
    transaction_id: Optional[str] = None
    variable: Optional[str] = None
    value: Optional[int] = None
    site_id: Optional[int] = None


class Parser:
    """Minimal parser for the supported command set."""

    BEGIN_PATTERN = re.compile(r'begin\s*\(\s*(\w+)\s*\)', re.IGNORECASE)
    READ_PATTERN = re.compile(r'R\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)', re.IGNORECASE)
    WRITE_PATTERN = re.compile(r'W\s*\(\s*(\w+)\s*,\s*(\w+)\s*,\s*(\d+)\s*\)', re.IGNORECASE)
    END_PATTERN = re.compile(r'end\s*\(\s*(\w+)\s*\)', re.IGNORECASE)
    FAIL_PATTERN = re.compile(r'fail\s*\(\s*(\d+)\s*\)', re.IGNORECASE)
    RECOVER_PATTERN = re.compile(r'recover\s*\(\s*(\d+)\s*\)', re.IGNORECASE)
    DUMP_PATTERN = re.compile(r'dump\s*\(\s*\)', re.IGNORECASE)
    QUERY_PATTERN = re.compile(r'querystate\s*\(\s*\)', re.IGNORECASE)

    @staticmethod
    def parse_line(line: str) -> Command:
        line = line.strip()
        # print(f"[debug] parse_line raw: {line}")

        if not line:
            return Command(cmd_type=CommandType.EMPTY)

        if line.startswith('//') or line.startswith('==='):
            return Command(cmd_type=CommandType.COMMENT)

        match = Parser.BEGIN_PATTERN.match(line)
        if match:
            return Command(
                cmd_type=CommandType.BEGIN,
                transaction_id=match.group(1)
            )

        match = Parser.READ_PATTERN.match(line)
        if match:
            return Command(
                cmd_type=CommandType.READ,
                transaction_id=match.group(1),
                variable=match.group(2)
            )

        match = Parser.WRITE_PATTERN.match(line)
        if match:
            return Command(
                cmd_type=CommandType.WRITE,
                transaction_id=match.group(1),
                variable=match.group(2),
                value=int(match.group(3))
            )

        match = Parser.END_PATTERN.match(line)
        if match:
            return Command(
                cmd_type=CommandType.END,
                transaction_id=match.group(1)
            )

        match = Parser.FAIL_PATTERN.match(line)
        if match:
            return Command(
                cmd_type=CommandType.FAIL,
                site_id=int(match.group(1))
            )

        match = Parser.RECOVER_PATTERN.match(line)
        if match:
            return Command(
                cmd_type=CommandType.RECOVER,
                site_id=int(match.group(1))
            )

        match = Parser.DUMP_PATTERN.match(line)
        if match:
            return Command(cmd_type=CommandType.DUMP)

        match = Parser.QUERY_PATTERN.match(line)
        if match:
            return Command(cmd_type=CommandType.QUERY_STATE)

        return Command(cmd_type=CommandType.COMMENT)

    @staticmethod
    def parse_file(filename: str) -> List[Command]:
        commands = []
        with open(filename, 'r') as f:
            for line in f:
                cmd = Parser.parse_line(line)
                commands.append(cmd)
        return commands
