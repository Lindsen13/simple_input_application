from flask import Flask, g, render_template, request

from utils import (
    close_connection,
    db_connect,
    get_all_people,
    load_people_from_file_to_db,
)

app = Flask(__name__)
app.teardown_appcontext(close_connection)


@app.route("/")
def index():
    """
    Renders the index.html template.

    Returns:
        Response: The rendered HTML template for the index page.
    """
    return render_template("index.html")


@app.route("/submit_file", methods=["POST"])
def submit_file():
    """
    Handle the submission of a file via a POST request to the /submit_file endpoint.

    This function processes the uploaded file, checks its validity, and loads its content
    into the database if it is a valid CSV file. It returns an appropriate HTML template
    based on the outcome of the file submission.

    Returns:
        str: Rendered HTML template indicating the result of the file submission.
    """
    file = request.files.get("file")

    if not file or file.filename == "":
        message = "No selected file" if file else "No file part"
    elif file.filename.endswith(".csv"):
        load_people_from_file_to_db(file, conn=db_connect())
        return render_template("file_submitted.html")
    else:
        message = "Invalid file type"

    return render_template("file_not_submitted.html", message=message)


@app.route("/people")
def people():
    """
    Fetches all people from the database and renders the 'people.html' template with the retrieved data.
    Returns:
        str: Rendered HTML template with the list of people.
    """

    people = get_all_people(conn=db_connect())
    return render_template("people.html", people=people)


if __name__ == "__main__":
    app.run(debug=True)
