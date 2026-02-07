import pytest
import os
import tempfile
import threading
import time
import requests
from otlp_mmap_example_server.app import create_app

@pytest.fixture(scope="module")
def mmap_file():
    # Create a temporary file for mmap data
    f = tempfile.NamedTemporaryFile(delete=False)
    path = f.name
    f.close() # Close so Flask app can open it
    yield path
    # Clean up the file after tests
    os.remove(path)

@pytest.fixture(scope="module")
def flask_app(mmap_file):
    # Set environment variable for the Flask app
    os.environ["SDK_MMAP_EXPORTER_FILE"] = mmap_file
    os.environ["OTLP_MMAP_SERVICE_NAME"] = "test-flask-server"
    
    app = create_app()
    app.config["TESTING"] = True
    
    # Suppress Flask's default logger output during tests
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    # Use a port that's likely free for the test server
    port = 5001 
    base_url = f"http://127.0.0.1:{port}"

    # Run Flask app in a separate thread
    thread = threading.Thread(target=lambda: app.run(host='127.0.0.1', port=port, debug=False, use_reloader=False))
    thread.daemon = True # Daemonize thread so it stops when main thread exits
    thread.start()
    
    # Wait for the server to start up
    time.sleep(2) 

    yield base_url
    
    # Clean up environment variables
    if "SDK_MMAP_EXPORTER_FILE" in os.environ:
        del os.environ["SDK_MMAP_EXPORTER_FILE"]
    if "OTLP_MMAP_SERVICE_NAME" in os.environ:
        del os.environ["OTLP_MMAP_SERVICE_NAME"]


def test_hello_world(flask_app, mmap_file):
    response = requests.get(f"{flask_app}/")
    assert response.status_code == 200
    assert "Hello, World!" in response.text
    
    # Minimal verification: check if mmap file has grown
    assert os.path.exists(mmap_file)
    assert os.path.getsize(mmap_file) > 0 

def test_fibonacci(flask_app, mmap_file):
    response = requests.get(f"{flask_app}/fib/5")
    assert response.status_code == 200
    assert "Fibonacci(5) = 5" in response.text
    
    assert os.path.exists(mmap_file)
    assert os.path.getsize(mmap_file) > 0
