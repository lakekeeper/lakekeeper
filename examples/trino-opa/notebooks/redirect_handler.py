from trino.auth import RedirectHandler, CompositeRedirectHandler, WebBrowserRedirectHandler

class DockerConsoleRedirectHandler(RedirectHandler):
    """
    Handler for OAuth redirections to log to console.
    """

    def __call__(self, url: str) -> None:
        print("Open the following URL in browser for the external authentication:")
        print(url.replace("trino-proxy", "localhost"))


REDIRECT_HANDLER = CompositeRedirectHandler([
        WebBrowserRedirectHandler(),
        DockerConsoleRedirectHandler()
])