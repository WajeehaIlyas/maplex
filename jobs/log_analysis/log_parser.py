# jobs/log_analysis/log_parser.py
# Regex patterns and helpers for parsing common log formats.
#
# Supported formats:
#   STANDARD  — "LEVEL message"  e.g. "ERROR disk failure"
#   APACHE    — Apache/Nginx combined access log
#   SYSLOG    — RFC 3164 syslog lines
#   TIMESTAMP — "YYYY-MM-DD HH:MM:SS LEVEL message"

import re
from typing import Optional


# ── Compiled patterns ─────────────────────────────────────────────────────────

# Simple: "LEVEL message"   (ERROR disk failure / INFO user login)
_STANDARD = re.compile(
    r"^\s*(?P<level>DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)"
    r"\s+(?P<message>.+)$", re.IGNORECASE)

# Timestamped: "2024-01-15 12:34:56 LEVEL message"
_TIMESTAMP = re.compile(
    r"^\s*(?P<ts>\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2})"
    r"\s+(?P<level>DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)"
    r"\s+(?P<message>.+)$", re.IGNORECASE)

# Bracketed level: "[ERROR] message" or "[2024-01-15] [ERROR] message"
_BRACKETED = re.compile(
    r"\[(?P<level>DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)\]"
    r"\s*(?P<message>.+)$", re.IGNORECASE)

# Apache/Nginx access log: IP - - [date] "METHOD /path HTTP/1.x" status bytes
_APACHE = re.compile(
    r"^(?P<ip>\d{1,3}(?:\.\d{1,3}){3})\s+-\s+-\s+"
    r'\[(?P<datetime>[^\]]+)\]\s+"(?P<method>\w+)\s+(?P<path>\S+)'
    r'\s+HTTP/\S+"\s+(?P<status>\d{3})\s+(?P<bytes>\d+|-)')

# Syslog: "Jan 15 12:34:56 hostname program[pid]: message"
_SYSLOG = re.compile(
    r"^(?P<month>\w{3})\s+(?P<day>\d{1,2})\s+(?P<time>\d{2}:\d{2}:\d{2})"
    r"\s+(?P<host>\S+)\s+(?P<program>[^:\[]+)(?:\[\d+\])?:\s+(?P<message>.+)$")

# ── Log levels normalisation ──────────────────────────────────────────────────

_LEVEL_MAP = {
    "warn"    : "WARNING",
    "warning" : "WARNING",
    "fatal"   : "CRITICAL",
    "debug"   : "DEBUG",
    "info"    : "INFO",
    "error"   : "ERROR",
    "critical": "CRITICAL",
}


def normalise_level(raw: str) -> str:
    return _LEVEL_MAP.get(raw.lower(), raw.upper())


# ── Public parse function ─────────────────────────────────────────────────────

def parse_line(line: str) -> Optional[dict]:
    """
    Try each pattern in order.  Return a dict on the first match, or None
    if the line is not recognised as a log entry.

    Returned dict always contains:
        "format"  : which pattern matched
        "level"   : normalised log level (or None for access logs)
        "message" : the log message body

    Access logs additionally contain: ip, method, path, status, bytes
    """
    line = line.strip()
    if not line:
        return None

    # Try timestamp first (more specific than standard)
    m = _TIMESTAMP.match(line)
    if m:
        return {"format": "TIMESTAMP",
                "level"  : normalise_level(m.group("level")),
                "message": m.group("message").strip(),
                "ts"     : m.group("ts")}

    m = _BRACKETED.search(line)
    if m:
        return {"format": "BRACKETED",
                "level"  : normalise_level(m.group("level")),
                "message": m.group("message").strip()}

    m = _STANDARD.match(line)
    if m:
        return {"format": "STANDARD",
                "level"  : normalise_level(m.group("level")),
                "message": m.group("message").strip()}

    m = _APACHE.match(line)
    if m:
        status = int(m.group("status"))
        level  = "ERROR" if status >= 500 else \
                 "WARNING" if status >= 400 else "INFO"
        return {"format": "APACHE",
                "level"  : level,
                "message": f'{m.group("method")} {m.group("path")} {status}',
                "ip"     : m.group("ip"),
                "method" : m.group("method"),
                "path"   : m.group("path"),
                "status" : status}

    m = _SYSLOG.match(line)
    if m:
        return {"format": "SYSLOG",
                "level"  : "INFO",
                "message": m.group("message").strip(),
                "host"   : m.group("host"),
                "program": m.group("program").strip()}

    return None   # unrecognised line
