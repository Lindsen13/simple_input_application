import datetime
import os
import sqlite3

import pytest

from src.utils import (
    Person,
    create_connection,
    create_table,
    get_all_people,
    insert_people,
    load_people_from_file_to_db,
    read_people_from_file,
)

TEST_DB_FILE = "test_people.db"


@pytest.fixture
def setup_test_db():
    """Setup a temporary database for testing"""
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)
    conn = sqlite3.connect(TEST_DB_FILE)
    create_table(conn)
    yield conn
    conn.close()
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)


def test_create_connection(setup_test_db):
    """Test the create_connection function with a temporary database"""
    conn = create_connection()
    assert conn is not None
    assert isinstance(conn, sqlite3.Connection)
    conn.close()


def test_create_table(setup_test_db):
    """Test the create_table function with a temporary database"""
    conn = setup_test_db
    create_table(conn)

    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='People';"
    )
    table_exists = cursor.fetchone()

    assert table_exists is not None
    assert table_exists[0] == "People"
    cursor.close()


def test_insert_people(setup_test_db):
    """Test the insert_people function with a temporary database"""
    conn = setup_test_db
    create_table(conn)

    people = [
        Person(name="Alice", date_of_birth="1990-01-01", weight=70.0, male=False),
        Person(name="Bob", date_of_birth="1985-05-15", weight=75.0, male=True),
    ]

    insert_people(conn, people)

    cursor = conn.cursor()
    cursor.execute("SELECT name, date_of_birth, weight, male FROM People;")
    rows = cursor.fetchall()

    assert len(rows) == 2
    assert rows[0] == ("Alice", "1990-01-01", 70.0, 0)
    assert rows[1] == ("Bob", "1985-05-15", 75.0, 1)
    cursor.close()


def test_insert_people_with_conflict(setup_test_db):
    """Test the insert_people function with conflicting entries"""
    conn = setup_test_db
    create_table(conn)

    people = [
        Person(name="Alice", date_of_birth="1990-01-01", weight=70.0, male=False),
        Person(name="Alice", date_of_birth="1990-01-01", weight=72.0, male=False),
    ]

    insert_people(conn, people)

    cursor = conn.cursor()
    cursor.execute("SELECT name, date_of_birth, weight, male FROM People;")
    rows = cursor.fetchall()

    assert len(rows) == 1
    assert rows[0] == ("Alice", "1990-01-01", 72.0, 0)
    cursor.close()


def test_read_people_from_file(tmp_path):
    """Test the read_people_from_file function"""
    test_file = tmp_path / "test_people.csv"
    test_file.write_text(
        "name,date_of_birth,weight,male\n"
        "Alice,1990-01-01,70.0,False\n"
        "Bob,1985-05-15,75.0,True\n"
    )

    people = read_people_from_file(test_file)

    assert len(people) == 2
    assert people[0].name == "Alice"
    assert people[0].date_of_birth == datetime.date(1990, 1, 1)
    assert people[0].weight == 70.0
    assert not people[0].male

    assert people[1].name == "Bob"
    assert people[1].date_of_birth == datetime.date(1985, 5, 15)
    assert people[1].weight == 75.0
    assert people[1].male


def test_load_people_from_file_to_db(tmp_path, setup_test_db):
    """Test the load_people_from_file_to_db function"""
    conn = setup_test_db

    test_file = tmp_path / "test_people.csv"
    test_file.write_text(
        "name,date_of_birth,weight,male\n"
        "Alice,1990-01-01,70.0,False\n"
        "Bob,1985-05-15,75.0,True\n"
    )

    load_people_from_file_to_db(test_file, conn=conn)

    cursor = conn.cursor()
    cursor.execute("SELECT name, date_of_birth, weight, male FROM People;")
    rows = cursor.fetchall()

    assert len(rows) == 2
    assert rows[0] == ("Alice", "1990-01-01", 70.0, 0)
    assert rows[1] == ("Bob", "1985-05-15", 75.0, 1)
    cursor.close()
    conn.close()


def test_get_all_people(setup_test_db):
    """Test the get_all_people function"""
    conn = setup_test_db
    create_table(conn)

    people = [
        Person(name="Alice", date_of_birth="1990-01-01", weight=70.0, male=False),
        Person(name="Bob", date_of_birth="1985-05-15", weight=75.0, male=True),
    ]

    insert_people(conn, people)

    all_people = get_all_people(conn)

    assert len(all_people) == 2
    assert all_people[0].name == "Alice"
    assert all_people[0].date_of_birth == datetime.date(1990, 1, 1)
    assert all_people[0].weight == 70.0
    assert not all_people[0].male

    assert all_people[1].name == "Bob"
    assert all_people[1].date_of_birth == datetime.date(1985, 5, 15)
    assert all_people[1].weight == 75.0
    assert all_people[1].male
