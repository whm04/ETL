from flask import Flask, jsonify
import sys, os


current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from database.models import Database
from config.config import DATABASE_URL, TABLE_NAME


app = Flask(__name__)


# Initialize database
db = Database(DATABASE_URL)


@app.route("/read/first-chunk", methods=["GET"])
def read_first_chunk():
    offers = db.get_first_10_offers()
    data = []
    for offer in offers:
        data.append(
            {
                "offer_number": offer.offer_number,
                "offer_address": offer.offer_address,
                "title": offer.title,
                "publication_date": offer.publication_date.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "language": offer.language,
                "soft_skills": offer.soft_skills,
                "hard_skills": offer.hard_skills,
                "salary": offer.salary,
                "company": offer.company,
                "profile": offer.profile,
            }
        )
    response_body = {"description": "A list of 10 JSON objects", "data": data}
    return jsonify(response_body), 200


if __name__ == "__main__":
    app.run(debug=False)
