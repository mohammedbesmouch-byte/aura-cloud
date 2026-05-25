
import http.server, json, os, socketserver, sys
PORT = int(os.environ.get("PORT", 8080))
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(s):
        s.send_response(200)
        s.send_header("Content-Type","application/json")
        s.end_headers()
        s.wfile.write(json.dumps({"hello":"world","port":PORT}).encode())
    def log_message(s,*a): pass
class T(socketserver.ThreadingMixIn,http.server.HTTPServer): pass
httpd = T(("0.0.0.0",PORT), H)
print(f"Listening on {PORT}", flush=True)
httpd.serve_forever()
