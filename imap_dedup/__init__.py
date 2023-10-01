#!/usr/bin/env python3

import click
from collections import defaultdict
from imapclient import IMAPClient
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional
import sqlite3
import logging
from tqdm import tqdm
from retrying import retry
from enum import Enum
from tabulate import tabulate

MESSAGE_ENVELOPE = "ENVELOPE"
MESSAGE_DATE = "INTERNALDATE"
MESSAGE_SIZE = "RFC822.SIZE"
MESSAGE_SUBJECT = "BODY.PEEK[HEADER.FIELDS (SUBJECT)]"
BATCH_FETCH_LIMIT = 25


@dataclass
class MessageRecord:
    folder: str
    seq: int
    subject: str
    message_id: str
    date_: datetime
    size: int


class Action(Enum):
    DELETE = 0


@dataclass
class ActionRecord:
    folder: str
    seq: int
    action: Action
    completed_at: datetime


class BadFetchError(Exception):
    pass


def _optional_utf8(item: Optional[bytes]) -> Optional[str]:
    if item is not None:
        return item.decode()
    else:
        return None


def create_message_record(folder: str, message: Dict) -> MessageRecord:
    seq = message[b"SEQ"]
    try:
        envelope = message[MESSAGE_ENVELOPE.encode()]
        date_ = message[MESSAGE_DATE.encode()]
        size = message[MESSAGE_SIZE.encode()]
    except KeyError:
        raise BadFetchError()

    return MessageRecord(
        folder=folder,
        seq=seq,
        subject=_optional_utf8(envelope.subject),
        message_id=_optional_utf8(envelope.message_id),
        date_=date_,
        size=size,
    )


def create_database_tables(cursor: sqlite3.Cursor) -> None:
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS messages (folder VARCHAR(128), seq INT, subject TEXT, message_id TEXT, date DATETIME, size INT, PRIMARY KEY (folder, seq))"
    )
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS actions (folder VARCHAR(128), seq INT, action TEXT, completed_at DATETIME, PRIMARY KEY (folder, seq))"
    )


detected_duplicates = False


def _is_unique_constraint_violation(e: sqlite3.IntegrityError) -> bool:
    return str(e).startswith("UNIQUE constraint failed")


def insert_message_record(
    cursor: sqlite3.Cursor, message_record: MessageRecord
) -> None:
    global detected_duplicates
    try:
        cursor.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)",
            [
                message_record.folder,
                message_record.seq,
                message_record.subject,
                message_record.message_id,
                message_record.date_,
                message_record.size,
            ],
        )
    except sqlite3.IntegrityError as e:
        if _is_unique_constraint_violation(e):
            if not detected_duplicates:
                logging.warning(
                    "Detected duplicates (example: folder=%s, seq=%d)",
                    message_record.folder,
                    message_record.seq,
                )
                detected_duplicates = True
        else:
            raise e


def message_record_exists(cursor: sqlite3.Cursor, folder: str, seq: int) -> bool:
    return (
        len(
            cursor.execute(
                "SELECT folder, seq FROM messages WHERE folder=? and seq=?",
                [folder, seq],
            ).fetchall()
        )
        > 0
    )


def insert_action_record(cursor: sqlite3.Cursor, action_record: ActionRecord) -> None:
    try:
        cursor.execute(
            "INSERT INTO actions (folder, seq, action) VALUES (?, ?, ?)",
            [action_record.folder, action_record.seq, action_record.action.name],
        )
    except sqlite3.IntegrityError as e:
        if not _is_unique_constraint_violation(e):
            raise e


def find_pending_action_records(cursor: sqlite3.Cursor) -> List[ActionRecord]:
    query = """
  SELECT
    folder,
    seq,
    action
  FROM actions
  WHERE completed_at is NULL
  """
    return [
        ActionRecord(r[0], r[1], r[2], None) for r in cursor.execute(query).fetchall()
    ]


def find_action_by_message(cursor: sqlite3.Cursor, message: MessageRecord):
    query = """
  SELECT
    action
  FROM actions
  WHERE
    folder = ? and
    seq = ? and
    completed_at is NULL
  """
    return [
        ActionRecord(
            folder=message.folder, seq=message.seq, action=r[0], completed_at=None
        )
        for r in cursor.execute(query, [message.folder, message.seq]).fetchall()
    ]


def find_duplicate_messages(cursor: sqlite3.Cursor) -> List[str]:
    query = """
  SELECT
    message_id,
    count(*) as cnt,
    size
  FROM messages
  WHERE message_id IS NOT NULL
  GROUP BY message_id, size
  ORDER BY cnt DESC, message_id ASC
  """
    records = cursor.execute(query).fetchall()
    return [r[0] for r in records if r[1] > 1]


def find_message_records_with_id(
    cursor: sqlite3.Cursor, message_id: str
) -> List[MessageRecord]:
    query = """
  SELECT
    folder,
    seq,
    subject,
    message_id,
    date,
    size
  FROM messages
  WHERE message_id=?
  """
    return [
        MessageRecord(r[0], r[1], r[2], r[3], r[4], r[5])
        for r in cursor.execute(query, [message_id]).fetchall()
    ]


def chunk(arr, limit):
    chunks = []
    while len(arr) > 0:
        chunks.append(arr[:limit])
        arr = arr[limit:]
    return chunks


@retry(
    retry_on_exception=lambda e: isinstance(e, BadFetchError),
    stop_max_attempt_number=10,
    wait_exponential_max=1000,
    wait_exponential_multiplier=50,
)
def load_messages_fetched_from_imap_with_retry(
    connection, server, message_seq_chunk, folder
):
    """
    :connection: The connection to the SQLite database
    :server: IMAP server connection
    """
    seqs_to_fetch = [
        s
        for s in message_seq_chunk
        if not message_record_exists(connection.cursor(), folder, s)
    ]

    messages = server.fetch(
        seqs_to_fetch, [MESSAGE_ENVELOPE, MESSAGE_DATE, MESSAGE_SIZE]
    )
    for seq in messages.keys():
        try:
            record = create_message_record(folder, messages[seq])
        except BadFetchError as e:
            logging.warning(
                "Detected bad fetch for folder %s, messages starting at %d", folder, seq
            )
            connection.rollback()
            raise e

        insert_message_record(connection.cursor(), record)

    connection.commit()


@dataclass
class CliContext:
    server: IMAPClient
    connection: sqlite3.Connection


@click.group()
@click.option(
    "--hostname", default="imap.mail.me.com", type=str, help="Hostname of IMAP server"
)
@click.option(
    "--username", required=True, type=str, help="Username for IMAP authentication"
)
@click.option(
    "--password", required=True, type=str, help="Password for IMAP authentication"
)
@click.option("--database", required=True, type=str, help="Database file")
@click.option("--verbose", type=bool, default=False, help="Enables verbose logging")
@click.pass_context
def cli(ctx, hostname: str, username: str, password: str, database: str, verbose: bool):
    logging.basicConfig(level=logging.INFO if not verbose else logging.DEBUG)

    server = IMAPClient(hostname, 993)
    server.login(username, password)
    logging.info("Connected to %s as %s", hostname, username)

    # import pdb; pdb.set_trace()

    connection = sqlite3.connect(database)
    create_database_tables(connection.cursor())

    ctx.obj = CliContext(server=server, connection=connection)


@cli.command(help="Pulls message metadata from IMAP server to determine duplicates")
@click.pass_context
def pull(ctx):
    ctx: CliContext = ctx.obj

    for folder in tqdm([f[2] for f in ctx.server.list_folders()], desc="Folders"):
        logging.debug(f"Processing {folder}")
        ctx.server.select_folder(folder)

        for message_seq_chunk in tqdm(
            chunk(ctx.server.search(), BATCH_FETCH_LIMIT),
            desc="Messages",
            colour="green",
            leave=False,
            unit_scale=BATCH_FETCH_LIMIT,
        ):
            load_messages_fetched_from_imap_with_retry(
                ctx.connection, ctx.server, message_seq_chunk, folder
            )


@cli.command(help="Finds duplicate messages and inserts them in the database")
@click.option("--all-mail", required=True, help="All mail folder")
@click.pass_context
def find_duplicates(ctx, all_mail: str):
    ctx: CliContext = ctx.obj

    logging.info("Will remove from folders %s", all_mail)

    cursor = ctx.connection.cursor()
    num_deletions = 0
    space_saved = 0
    for index, id_ in enumerate(tqdm(find_duplicate_messages(cursor))):
        for duplicate in [
            d for d in find_message_records_with_id(cursor, id_) if d.folder == all_mail
        ]:
            insert_action_record(
                cursor,
                ActionRecord(
                    folder=duplicate.folder,
                    seq=duplicate.seq,
                    action=Action.DELETE,
                    completed_at=None,
                ),
            )
            num_deletions += 1
            space_saved += duplicate.size

        if index % 100 == 0:
            ctx.connection.commit()

    ctx.connection.commit()

    logging.info(
        "Found %d duplicate messages, will perform %d deletions saving %d bytes of space",
        index,
        num_deletions,
        space_saved,
    )
    print(
        f"Found {index} duplicate messages, will perform {num_deletions} deletions saving {space_saved} bytes of space"
    )


@cli.command(help="Prints duplicates discovered by the find_duplicates command")
@click.pass_context
def print_duplicates(ctx):
    ctx: CliContext = ctx.obj

    cursor = ctx.connection.cursor()
    data = []
    for id_ in tqdm(find_duplicate_messages(cursor)):
        messages = find_message_records_with_id(cursor, id_)

        actions = []
        for message in messages:
            action = find_action_by_message(cursor, message)
            actions.append(
                f'{message.folder} : {action[0].action if len(action) > 0 else "KEEP"}'
            )

        data.append([messages[0].subject, "\n".join(actions)])

    print(tabulate(data, headers=["Subject", "Folders"]))


@cli.command(help="Performs deduplication")
@click.pass_context
def deduplicate(ctx):
    ctx: CliContext = ctx.obj
    cursor = ctx.connection.cursor()

    folder_to_actions = defaultdict(list)
    for action in find_pending_action_records(cursor):
        folder_to_actions[action.folder].append(action)

    for folder, actions in tqdm(folder_to_actions.items(), desc="Folders"):
        ctx.server.select_folder(folder)

        chunks = chunk(actions, BATCH_FETCH_LIMIT)
        for chunk_ in tqdm(
            chunks, desc="Messages", colour="red", unit_scale=BATCH_FETCH_LIMIT
        ):
            assert len(chunk_) > 0, "Expected chunk to have elements in it"
            ctx.server.delete_messages([r.seq for r in chunk_])

        ctx.server.expunge()


if __name__ == "__main__":
    cli()
