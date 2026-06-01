__package__ = "abx_dl"


def main(*args, **kwargs):
    """Run the abx-dl CLI entrypoint."""
    from .cli import main as cli_main

    return cli_main(*args, **kwargs)
