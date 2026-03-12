from web.app_factory import create_app

app = create_app()


def run_web_app() -> None:
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
        use_reloader=False
    )


if __name__ == "__main__":
    run_web_app()
