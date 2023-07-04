import json
from pathlib import Path

import click
from prometheus_client import start_http_server

from ._main import collect


@click.command()
@click.option('--config', type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option('--host', type=str, default='127.0.0.1')
@click.option('--port', type=int, default=7900)
@click.option('--interval', type=int, default=60)
@click.option('--debug', is_flag=True)
def cli(config: Path, host: str, port: int, interval: int, debug: bool) -> None:
    click.echo(f'Start collect metrics ...')
    config_data = json.loads(config.read_text())
    start_http_server(port=port, addr=host)
    collect(config_data, interval, debug)
