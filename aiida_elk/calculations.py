"""
Calculations provided by aiida_elk.

Register calculations via the "aiida.calculations" entry point in setup.json.
"""

from aiida.common import datastructures
from aiida.engine import CalcJob
from aiida.orm import SinglefileData
from aiida.plugins import DataFactory
from aiida import orm
from aiida_elk.data.lapwbasis import LapwbasisData
from ase.io.elk import write_elk_in
from aiida.orm import Group
import os


class ElkCalculation(CalcJob):
    """
    AiiDA calculation plugin wrapping the Elk executable.

    Simple AiiDA plugin for 'Elk'
    """

    _SPECIES_SUBFOLDER = "species"

    _LIST_OF_OUTPUTS = [
        "DTOTENERGY.OUT",
        "EFERMI.OUT",
        "EIGVAL.OUT",
        "EQATOMS.OUT",
        "EVALCORE.OUT",
        "EVALFV.OUT",
        "EVALSV.OUT",
        "FERMIDOS.OUT",
        "GAP.OUT",
        "GEOMETRY.OUT",
        "IADIST.OUT",
        "INFO.OUT",
        "KPOINTS.OUT",
        "LATTICE.OUT",
        "LINENGY.OUT",
        "OCCSV.OUT",
        "RMSDVS.OUT",
        "SYMCRYS.OUT",
        "SYMLAT.OUT",
        "SYMSITE.OUT",
        "TOTENERGY.OUT",
    ]

    @classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation."""
        super().define(spec)

        # set default values for AiiDA options
        spec.inputs["metadata"]["options"]["resources"].default = {
            "num_machines": 1,
            "num_mpiprocs_per_machine": 1,
        }
        spec.inputs["metadata"]["options"]["parser_name"].default = "elk"

        # new ports
        spec.input("metadata.options.input_filename", valid_type=str, default="elk.in")
        spec.input(
            "metadata.options.output_filename", valid_type=str, default="elk.out"
        )

        spec.input(
            "structure", valid_type=orm.StructureData, help="The input structure."
        )
        spec.input(
            "lapwbasis", valid_type=orm.Str, help="The name of the LAPW basis family"
        )

        spec.output(
            "output_parameters",
            valid_type=orm.Dict,
            help="The `output_parameters` output node of the successful calculation.",
        )

        spec.default_output_node = "output_parameters"

        spec.exit_code(
            300,
            "ERROR_MISSING_OUTPUT_FILES",
            message="Calculation did not produce all expected output files.",
        )

    def prepare_for_submission(self, folder):
        """
        Create input files.

        :param folder: an `aiida.common.folders.Folder` where the plugin should temporarily place all files
            needed by the calculation.
        :return: `aiida.common.datastructures.CalcInfo` instance
        """
        codeinfo = datastructures.CodeInfo()

        codeinfo.code_uuid = self.inputs.code.uuid
        codeinfo.stdout_name = self.metadata.options.output_filename

        # Prepare a `CalcInfo` to be returned to the engine
        calcinfo = datastructures.CalcInfo()
        calcinfo.codes_info = [codeinfo]

        # find chemical symbols appearing in the structure
        species = self.inputs.structure.get_symbols_set()
        # get group containing all species files (this should be replaced with get_lapwbasis_group)
        lapwbasis_group = Group.collection.get(label=self.inputs.lapwbasis.value)

        # add species files reuired for this calculation
        local_copy_list = []
        for specie in species:
            for node in lapwbasis_group.nodes:
                if isinstance(node, LapwbasisData):
                    if node.chemical_symbol == specie:
                        local_copy_list.append(
                            (
                                node.uuid,
                                node.filename,
                                os.path.join(self._SPECIES_SUBFOLDER, node.filename),
                            )
                        )

        # add species files to the list
        calcinfo.local_copy_list = local_copy_list

        # Use ASE to prepare elk's input
        with folder.open(self.metadata.options.input_filename, "w") as handle:
            write_elk_in(
                handle,
                self.inputs.structure.get_ase(),
                {"tasks": "0", "species_dir": "./species/"},
            )

        # list of files required for parsing (actually more than that)
        calcinfo.retrieve_list = [self.metadata.options.output_filename]
        calcinfo.retrieve_list += self._LIST_OF_OUTPUTS

        return calcinfo
