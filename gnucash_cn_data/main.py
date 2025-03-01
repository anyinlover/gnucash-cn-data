from pathlib import Path
import tomllib
import typer
from importer.base import Base
from importer.helper import create_class_map_and_import

app = typer.Typer()


@app.command()
def load(kind: str, csv_path: str):
    with open(Path.home() / ".config/gnucash-cn-data/accounts.toml", "rb") as f:
        accounts_map = tomllib.load(f)
    with open(Path.home() / ".config/gnucash-cn-data/filter.toml", "rb") as f:
        filters_map = tomllib.load(f)
    with open(Path.home() / ".config/gnucash-cn-data/env.toml", "rb") as f:
        env_map = tomllib.load(f)

    class_map = create_class_map_and_import(Base)
    importer = class_map[kind](Path(csv_path), accounts_map, filters_map, env_map)
    importer()


if __name__ == "__main__":
    app()
