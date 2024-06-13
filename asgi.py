# asgi.py

from flask import Flask
from main import app  # Import your Flask app from dummy_api.py

# Create an ASGI-compatible application callable
asgi_app = Flask(__name__)
asgi_app.wsgi_app = app

# Define an entry point for ASGI server
def app(scope):
    async def asgi_app_receive():
        pass

    async def asgi_app_send(message):
        pass

    return asgi_app_receive, asgi_app_send

app = asgi_app  # ASGI entry point for Vercel
