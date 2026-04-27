#!/usr/bin/env python3
"""
Dashboard server - serves dashboard.html and runs Python scrapers via API.
"""

import json
import subprocess
import threading
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from datetime import datetime, timedelta
import sys
import os

PORT = 8000
DIRECTORY = os.path.dirname(os.path.abspath(__file__))

SCRAPER_METHODS = {
    "playwright": "scraper_playwright.py",
    "selenium": "scraper_selenium.py",
    "pyautogui": "scraper_pyautogui.py",
}

# Store run state globally
run_state = {
    "running": False,
    "results": [],
    "logs": [],
}

class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_POST(self):
        if self.path == "/api/run":
            self.handle_run()
        elif self.path == "/api/status":
            self.handle_status()
        elif self.path == "/api/stats":
            self.handle_stats()
        elif self.path == "/api/stop":
            self.handle_stop()
        else:
            self.send_error(404)

    def do_GET(self):
        if self.path == "/api/logs":
            self.handle_logs()
        else:
            super().do_GET()

    def handle_run(self):
        global run_state
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        data = json.loads(body) if body else {}

        method = data.get("method", "playwright")
        hour = int(data.get("hour", 10))

        if run_state["running"]:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Already running"}).encode())
            return

        run_state["running"] = True
        run_state["results"] = []
        run_state["logs"] = []

        def run_scraper():
            global run_state
            try:
                script = SCRAPER_METHODS.get(method)
                if not script:
                    raise ValueError(f"Unknown method: {method}")

                cmd = [sys.executable, script, "--hour", str(hour)]
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )

                for line in process.stdout:
                    if "ERROR" in line or "error" in line:
                        run_state["logs"].append({"ts": datetime.now().strftime("%H:%M:%S"), "msg": line.strip(), "type": "error"})
                    elif "Done" in line or "complete" in line.lower():
                        run_state["logs"].append({"ts": datetime.now().strftime("%H:%M:%S"), "msg": line.strip(), "type": "success"})
                    elif "Starting" in line:
                        run_state["logs"].append({"ts": datetime.now().strftime("%H:%M:%S"), "msg": line.strip(), "type": "info"})
                    else:
                        run_state["logs"].append({"ts": datetime.now().strftime("%H:%M:%S"), "msg": line.strip(), "type": "normal"})

                process.wait()

                # Load results
                try:
                    with open("rev.json") as f:
                        rev_data = json.load(f)
                    run_state["results"] = rev_data if isinstance(rev_data, dict) else {}
                except:
                    run_state["results"] = {}

            except Exception as e:
                run_state["logs"].append({"ts": datetime.now().strftime("%H:%M:%S"), "msg": str(e), "type": "error"})
            finally:
                run_state["running"] = False

        thread = threading.Thread(target=run_scraper)
        thread.start()

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "started"}).encode())

    def handle_stop(self):
        global run_state
        run_state["running"] = False
        run_state["logs"].append({"ts": datetime.now().strftime("%H:%M:%S"), "msg": "Stopped by user", "type": "warn"})
        
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"status": "stopped"}).encode())

    def handle_status(self):
        global run_state
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"running": run_state["running"]}).encode())

    def handle_stats(self):
        global run_state
        stats = {"total": 0, "deleted": 0, "added": 0}
        
        try:
            with open("rev.json") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    stats["total"] = len(data)
        except:
            pass
        
        try:
            with open("deleted.json") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    stats["deleted"] = len(data)
        except:
            pass

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(stats).encode())

    def handle_logs(self):
        global run_state
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(run_state["logs"]).encode())

    def log_message(self, format, *args):
        pass  # Suppress HTTP logging


def main():
    server = HTTPServer(("0.0.0.0", PORT), DashboardHandler)
    print(f"\n🚀 Dashboard server running at http://localhost:{PORT}")
    print(f"   Open http://localhost:{PORT}/dashboard.html in your browser")
    print(f"   Press Ctrl+C to stop\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n👋 Server stopped")
        server.shutdown()


if __name__ == "__main__":
    main()