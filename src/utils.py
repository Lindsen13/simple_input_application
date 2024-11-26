import sqlite3
from datetime import date, datetime
from sqlite3 import Error
from typing import Optional

import pandas as pd
from flask import g
from pydantic import BaseModel


class Person(BaseModel):
    """Person model"""

    id: Optional[int] = None
    name: str
    date_of_birth: date
    weight: float
    male: bool
    date_created: Optional[datetime] = None
    date_modified: Optional[datetime] = None


REQUIRED_COLUMNS = {"name", "date_of_birth", "weight", "male"}

BD_FILE = "people.db"


def db_connect() -> sqlite3.Connection:
    """
    Establishes a database connection if one does not already exist in the global context.

    This function checks if a database connection is already present in the global context `g`.
    If not, it creates a new connection and stores it in `g.conn`. The connection is then returned.

    Returns:
        Connection: A database connection object.
    """
    if "conn" not in g:
        g.conn = create_connection()
    return g.conn


def close_connection(exception: Optional[Exception] = None) -> None:
    """
    Closes the database connection if it exists.

    Args:
        exception (Optional[Exception]): An optional exception that might have occurred. Defaults to None.

    Returns:
        None
    """
    conn = g.pop("conn", None)
    if conn is not None:
        conn.close()


def create_connection() -> sqlite3.Connection:
    """create a database connection to a SQLite database"""
    conn = None
    try:
        conn = sqlite3.connect(BD_FILE)
        print(f"Connection to {BD_FILE} established.")
    except Error as e:
        print(e)
    return conn


def create_table(conn: sqlite3.Connection) -> None:
    """create a table if it does not exist"""
    try:
        sql_create_people_table = """ 
        CREATE TABLE IF NOT EXISTS People (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            date_of_birth DATE NOT NULL,
            weight REAL NOT NULL,
            male BOOLEAN NOT NULL,
            date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            date_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(name, date_of_birth)
        );
        """
        cursor = conn.cursor()
        cursor.execute(sql_create_people_table)
        cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_date_modified AFTER UPDATE ON People
        FOR EACH ROW 
            WHEN NEW.date_of_birth != OLD.date_of_birth 
            OR NEW.weight != OLD.weight 
            OR NEW.male != OLD.male
        BEGIN
        UPDATE People 
            SET date_modified = CURRENT_TIMESTAMP 
            WHERE id = OLD.id;
        END;
        """)
        print("Table 'People' created or already exists.")
    except Error as e:
        print(e)


def insert_people(conn: sqlite3.Connection, people: list[Person]) -> None:
    """insert a list of people into the People table"""
    sql_insert_person = """ INSERT INTO People(name, date_of_birth, weight, male)
                            VALUES(?, ?, ?, ?)
                            ON CONFLICT(name, date_of_birth) DO UPDATE SET
                            weight=excluded.weight,
                            male=excluded.male """
    try:
        cursor = conn.cursor()
        cursor.executemany(
            sql_insert_person,
            [
                (person.name, person.date_of_birth, person.weight, person.male)
                for person in people
            ],
        )
        conn.commit()
        print(f"{len(people)} people inserted into the table.")
    except Error as e:
        print(e)
        conn.rollback()
    finally:
        cursor.close()


def read_people_from_file(file: str) -> list[Person]:
    """read a FileStorage object into a list of Person using pandas"""
    people = []
    df = pd.read_csv(file)
    if not REQUIRED_COLUMNS.issubset(df.columns):
        raise ValueError(
            f"Missing required columns: {REQUIRED_COLUMNS - set(df.columns)}"
        )
    for _, row in df.iterrows():
        person = Person(
            name=row["name"],
            date_of_birth=row["date_of_birth"],
            weight=row["weight"],
            male=row["male"],
        )
        people.append(person)
    return people


def load_people_from_file_to_db(file: str, conn: sqlite3.Connection) -> None:
    """read people from a file and insert them into the database"""
    create_table(conn)
    people = read_people_from_file(file)
    insert_people(conn, people)


def get_all_people(conn: sqlite3.Connection) -> list[Person]:
    """fetch all people from the People table"""
    cursor = conn.cursor()

    cursor.execute(
        "SELECT id, name, date_of_birth, weight, male, date_created, date_modified FROM People"
    )
    rows = cursor.fetchall()

    people = [
        Person(
            id=row[0],
            name=row[1],
            date_of_birth=row[2],
            weight=row[3],
            male=row[4],
            date_created=row[5],
            date_modified=row[6],
        )
        for row in rows
    ]
    cursor.close()
    return people
