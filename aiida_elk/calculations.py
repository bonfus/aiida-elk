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

    _LIST_OF_OPTIONAL_OUTPUTS = [
        "GEOMETRY_OPT",
        "IADIST_OPT.OUT",
        "FORCES_OPT.OUT",
        "TOTENERGY_OPT.OUT"
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
        spec.input('parameters', valid_type=orm.Dict,
            help='The input parameters that are to be used to construct the input file.')

        spec.output(
            "output_parameters",
            valid_type=orm.Dict,
            help="The `output_parameters` output node of the successful calculation.",
        )

        spec.output(
            "output_structure",
            valid_type=orm.StructureData,
            help="The`output_structure` output node providing the optimized structure of the successful calculation.",
        )

        spec.default_output_node = "output_parameters"

        spec.exit_code(
            300,
            "ERROR_MISSING_OUTPUT_FILES",
            message="Calculation did not produce all expected output files.",
        )

        spec.exit_code(
            410,
            'ERROR_ELECTRONIC_CONVERGENCE_NOT_REACHED',
            message='The electronic minimization cycle did not reach self-consistency.',
        )
        spec.exit_code(
            500,
            'ERROR_IONIC_CONVERGENCE_NOT_REACHED',
            message='The ionic minimization cycle did not converge for the given thresholds.',
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

        # create dictionary with additional settings
        parameters = self.inputs.parameters.get_dict()
        input_params = _lowercase_dict(parameters, dict_name='parameters')
        input_params['tasks'] = input_params.get('tasks', '0')
        input_params['species_dir'] = "./species/"

        # Use ASE to prepare elk's input
        with folder.open(self.metadata.options.input_filename, "w") as handle:
            write_elk_in(
                handle,
                self.inputs.structure.get_ase(),
                input_params,
            )

        # list of files required for parsing (actually more than that)
        calcinfo.retrieve_list = [self.metadata.options.output_filename]
        calcinfo.retrieve_list += self._LIST_OF_OUTPUTS

        return calcinfo



def _lowercase_dict(dictionary, dict_name):
    return _case_transform_dict(dictionary, dict_name, '_lowercase_dict', str.lower)


def _uppercase_dict(dictionary, dict_name):
    return _case_transform_dict(dictionary, dict_name, '_uppercase_dict', str.upper)


def _case_transform_dict(dictionary, dict_name, func_name, transform):
    from collections import Counter

    if not isinstance(dictionary, dict):
        raise TypeError(f'{func_name} accepts only dictionaries as argument, got {type(dictionary)}')
    new_dict = dict((transform(str(k)), v) for k, v in dictionary.items())
    if len(new_dict) != len(dictionary):
        num_items = Counter(transform(str(k)) for k in dictionary.keys())
        double_keys = ','.join([k for k, v in num_items if v > 1])
        raise exceptions.InputValidationError(
            f'Inside the dictionary `{dict_name}` there are the following keys that are repeated more than once when '
            f'compared case-insensitively: {double_keys}. This is not allowed.'
        )
    return new_dict
