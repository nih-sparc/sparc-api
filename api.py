# api.py

from flask import Flask
from .config import Config
from .email_sender import EmailSender
import json 
import request
from .serializer import ContactRequestSchema


app = Flask(__name__)

@app.route('/')
def hello_world():
    return Config.BLACKFYNN_API_HOST

@app.route("/contact", methods=["POST"])
def contact():
    data = json.loads(request.data)
    contact_request = ContactRequestSchema().load(data).data

    name = contact_request["name"]
    email = contact_request["email"]
    message = contact_request["message"]

    email_sender.send_email(name, email, message)

    return json.dumps({ "status": "sent" })