import sqlite3
from typing import List, Optional


class DBManager:
    def __init__(self, db_path: str) -> None:
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        with self.db:
            self.db.execute(
                """CREATE TABLE IF NOT EXISTS accounts
                (addr TEXT PRIMARY KEY,
                phone TEXT NOT NULL,
                password TEXT)"""
            )

    def add_account(self, addr: str, phone: str, password: str = None) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO accounts VALUES (?,?,?)", (addr, phone, password or "")
            )

    def get_account(self, addr: str) -> Optional[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM accounts WHERE addr=?", (addr,)
        ).fetchone()

    def set_password(self, addr: str, password: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE accounts SET password=? WHERE addr=?", (password, addr)
            )

    def delete_account(self, addr) -> None:
        with self.db:
            self.db.execute("DELETE FROM accounts WHERE addr=?", (addr,))
