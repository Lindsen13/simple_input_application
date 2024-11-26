import io

import pytest
from pytest import MonkeyPatch

from src.main import app

from flask.testing import FlaskClient
from typing import Any, Generator

@pytest.fixture
def client()-> Generator[FlaskClient, Any, None]:
    with app.test_client() as client:
        with app.app_context():
            yield client


def test_index_route(client:FlaskClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert b"Upload Your File" in response.data


def test_submit_file_no_file(client:FlaskClient) -> None:
    response = client.post("/submit_file", data={})
    assert response.status_code == 200
    assert b"No file part" in response.data


def test_submit_file_no_selected_file(client:FlaskClient) -> None:
    data = {"file": (None, "")}
    response = client.post("/submit_file", data=data)
    assert response.status_code == 200
    assert b"No file part" in response.data


def test_submit_file_invalid_file_type(client:FlaskClient) -> None:
    data = {"file": (io.BytesIO(b"some content"), "test.txt")}
    response = client.post(
        "/submit_file", data=data, content_type="multipart/form-data"
    )
    assert response.status_code == 200
    assert b"Invalid file type" in response.data


def test_people_route(client: FlaskClient, monkeypatch: MonkeyPatch) -> None:
    def mock_get_all_people(conn):
        return [{"name": "John Doe", "age": 30}, {"name": "Jane Doe", "age": 25}]

    monkeypatch.setattr("src.main.get_all_people", mock_get_all_people)

    response = client.get("/people")
    assert response.status_code == 200
    assert b"John Doe" in response.data
    assert b"Jane Doe" in response.data
