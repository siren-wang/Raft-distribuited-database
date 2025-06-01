#!/usr/bin/env python3
"""
Simple HTTP server with CORS support for the Raft dashboard
"""
import http.server
import socketserver
from http.server import SimpleHTTPRequestHandler

class CORSRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

if __name__ == "__main__":
    PORT = 3000
    
    with socketserver.TCPServer(("", PORT), CORSRequestHandler) as httpd:
        print(f"Serving at http://localhost:{PORT}")
        print(f"Dashboard available at: http://localhost:{PORT}/raft-dashboard.html")
        httpd.serve_forever() 