"""
Command line interface (cli) for aiida_elk.

Register new commands either via the "console_scripts" entry point or plug them
directly into the 'verdi' command by using AiiDA-specific entry points like
"aiida.cmdline.data" (both in the setup.json file).
"""

import sys
import click

from aiida_elk.data.lapwbasis import upload_family


@click.group()
def lapwbasis():
    """Help for lapwbasis command"""


@lapwbasis.command("upload")
@click.option("--name", type=str, help="Name of the LAPW basis set", required=True)
@click.option("--description", type=str, help="Description of the set", required=False)
@click.argument("path", type=str, required=True)
def upload_command(path, name, description):
    """Upload a new set of LAPW basis files"""
    import os.path

    stop_if_existing = False

    folder = os.path.abspath(path)

    if not os.path.isdir(folder):
        print(sys.stderr, "Cannot find directory: " + folder)
        sys.exit(1)

    if not description:
        description = ""

    files_found, files_uploaded = upload_family(
        folder, name, description, stop_if_existing
    )

    print(
        "Species files found: {}. New files uploaded: {}".format(
            files_found, files_uploaded
        )
    )


@lapwbasis.command("list")
def list_command():
    """List the uploaded sets of LAPW basis files"""

    with_description = True

    from aiida.plugins import DataFactory

    LapwbasisData = DataFactory("elk.lapwbasis")
    groups = LapwbasisData.get_lapwbasis_groups()

    if groups:
        for g in groups:
            sp = LapwbasisData.query(dbgroups=g.dbgroup).distinct()
            num_sp = sp.count()

            if with_description:
                description_string = ": {}".format(g.description)
            else:
                description_string = ""

            print("* {} [{} species]{}".format(g.name, num_sp, description_string))
    else:
        print("No LAPW basis sets were found.")
