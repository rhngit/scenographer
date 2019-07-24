#!/usr/bin/env python


"""
Scenographer - The cool dude who sets up the stage. Word.

Usage:
  scenographer sample <config-path> [--skip-schema]
  scenographer empty-config
  scenographer -h | --help
  scenographer --version

Options:
  -h --help                Show this screen.
  --version                Show version.
"""

from docopt import docopt

from scenographer.scenographer import Scenographer, Settings


def cli():
    args = docopt(__doc__, version="0.2.0")

    if args["sample"] is True:
        scenographer = Scenographer(options=Settings.load(args["<config-path>"]))

        if args["--skip-schema"] is False:
            scenographer.copy_schema()

        scenographer.copy_sample()

    elif args["empty-config"]:
        print(Settings.empty().json)
