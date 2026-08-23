from __future__ import annotations

from waitress import serve

from streamchart.api.app import create_app
from streamchart.config import settings
from streamchart.logging import configure_logging, get_logger

log = get_logger("streamchart.api")


def main() -> None:  # pragma: no cover - server wiring
    configure_logging(settings.log_level)
    app = create_app()
    log.info(
        "starting api on %s:%s",
        settings.api_listen_address,
        settings.api_port,
    )
    serve(app, host=settings.api_listen_address, port=settings.api_port)


if __name__ == "__main__":  # pragma: no cover
    main()
