from dataclasses import dataclass


@dataclass
class NotFoundError(Exception):
    help_msg: str


@dataclass
class IncorrectHttpStatus(Exception):
    help_msg: str


@dataclass
class InvalidCloseError(Exception):
    help_msg: str
