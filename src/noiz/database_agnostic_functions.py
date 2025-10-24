# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

"""
Database-agnostic SQL functions for cross-database compatibility.

This module provides custom SQLAlchemy functions that compile to different
SQL dialects based on the database backend (PostgreSQL vs SQLite).
"""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import FunctionElement


class ExtractYear(FunctionElement):
    """Extract year from a timestamp column."""

    name = "extract_year"
    inherit_cache = True


@compiles(ExtractYear, "sqlite")
def visit_extract_year_sqlite(element, compiler, **kw):
    """Compile to SQLite's strftime function."""
    return "CAST(strftime('%%Y', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


@compiles(ExtractYear, "postgresql")
def visit_extract_year_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's date_part function."""
    return "CAST(date_part('year', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


class ExtractDOY(FunctionElement):
    """Extract day-of-year from a timestamp column."""

    name = "extract_doy"
    inherit_cache = True


@compiles(ExtractDOY, "sqlite")
def visit_extract_doy_sqlite(element, compiler, **kw):
    """Compile to SQLite's strftime function."""
    return "CAST(strftime('%%j', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


@compiles(ExtractDOY, "postgresql")
def visit_extract_doy_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's date_part function."""
    return "CAST(date_part('doy', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


class ExtractISOWeekday(FunctionElement):
    """Extract ISO weekday (1=Monday, 7=Sunday) from a timestamp column."""

    name = "extract_isoweekday"
    inherit_cache = True


@compiles(ExtractISOWeekday, "sqlite")
def visit_extract_isoweekday_sqlite(element, compiler, **kw):
    """Compile to SQLite's strftime function.

    SQLite's %w returns 0-6 (0=Sunday), we need 1-7 (1=Monday).
    Formula: ((strftime('%w') + 6) % 7) + 1
    """
    return "((CAST(strftime('%%w', %s) AS INTEGER) + 6) %% 7) + 1" % compiler.process(element.clauses, **kw)


@compiles(ExtractISOWeekday, "postgresql")
def visit_extract_isoweekday_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's date_part function."""
    return "CAST(date_part('isodow', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


class ExtractHour(FunctionElement):
    """Extract hour from a timestamp column."""

    name = "extract_hour"
    inherit_cache = True


@compiles(ExtractHour, "sqlite")
def visit_extract_hour_sqlite(element, compiler, **kw):
    """Compile to SQLite's strftime function."""
    return "CAST(strftime('%%H', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


@compiles(ExtractHour, "postgresql")
def visit_extract_hour_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's date_part function."""
    return "CAST(date_part('hour', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


class ExtractMonth(FunctionElement):
    """Extract month from a timestamp column."""

    name = "extract_month"
    inherit_cache = True


@compiles(ExtractMonth, "sqlite")
def visit_extract_month_sqlite(element, compiler, **kw):
    """Compile to SQLite's strftime function."""
    return "CAST(strftime('%%m', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


@compiles(ExtractMonth, "postgresql")
def visit_extract_month_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's date_part function."""
    return "CAST(date_part('month', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


class ExtractDay(FunctionElement):
    """Extract day of month from a timestamp column."""

    name = "extract_day"
    inherit_cache = True


@compiles(ExtractDay, "sqlite")
def visit_extract_day_sqlite(element, compiler, **kw):
    """Compile to SQLite's strftime function."""
    return "CAST(strftime('%%d', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


@compiles(ExtractDay, "postgresql")
def visit_extract_day_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's date_part function."""
    return "CAST(date_part('day', %s) AS INTEGER)" % compiler.process(element.clauses, **kw)


class RightSubstring(FunctionElement):
    """Extract N rightmost characters from a string column."""

    name = "right_substring"
    inherit_cache = True


@compiles(RightSubstring, "sqlite")
def visit_right_substring_sqlite(element, compiler, **kw):
    """Compile to SQLite's substr function.

    SQLite substr(string, start, length) where negative start counts from end.
    """
    args = list(element.clauses)
    if len(args) != 2:
        raise ValueError("RightSubstring requires exactly 2 arguments: column and length")
    column = compiler.process(args[0], **kw)
    length = compiler.process(args[1], **kw)
    return f"substr({column}, -{length})"


@compiles(RightSubstring, "postgresql")
def visit_right_substring_postgresql(element, compiler, **kw):
    """Compile to PostgreSQL's right function."""
    args = list(element.clauses)
    if len(args) != 2:
        raise ValueError("RightSubstring requires exactly 2 arguments: column and length")
    column = compiler.process(args[0], **kw)
    length = compiler.process(args[1], **kw)
    return f"right({column}, {length})"
