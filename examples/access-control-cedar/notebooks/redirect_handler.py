from trino.auth import RedirectHandler, CompositeRedirectHandler, WebBrowserRedirectHandler

class DockerConsoleRedirectHandler(RedirectHandler):
    """
    Handler for OAuth redirections to log to console.
    We replace "https://trino-proxy" with "http://localhost:38191" because:
    * trino-proxy is a docker internal name. The users browser can't reach it
    * | we don't have a trusted certificate, so we don't want to use https. 
      | Instead we use port 38191 which is served by nginx and always sets the 
      | X-Forwarded-Proto https header required by trino (even for http connections). 
      | Use a proper certificate in production instead!!
    """

    def __call__(self, url: str) -> None:
        print("Open the following URL in browser for the external authentication:")
        print(url.replace("https://trino-proxy", "http://localhost:38191"))


REDIRECT_HANDLER = CompositeRedirectHandler([
        WebBrowserRedirectHandler(),
        DockerConsoleRedirectHandler()
])