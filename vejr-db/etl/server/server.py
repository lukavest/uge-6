
from http.server import BaseHTTPRequestHandler, HTTPServer

import matplotlib
matplotlib.use("Agg")


from etl.server.plot import build_plot


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/":
            html = """
            <html>
              <body>
                <h1>Plot page</h1>
                <img src="/plot.png" alt="Plot">
              </body>
            </html>
            """
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return

        if self.path == "/plot.png":
            image = build_plot()
            self.send_response(200)
            self.send_header("Content-type", "image/png")
            self.end_headers()
            self.wfile.write(image)
            return

        self.send_response(404)
        self.end_headers()


def run() -> None:
    server = HTTPServer(("127.0.0.1", 8000), RequestHandler)
    print("Server running at http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    run()