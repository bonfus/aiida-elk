"""
Parsers provided by aiida_elk.

Register parsers via the "aiida.parsers" entry point in setup.json.
"""

from aiida.common import exceptions
from aiida.engine import ExitCode
from aiida.orm import Dict
from aiida.parsers.parser import Parser
from aiida.plugins import CalculationFactory
import tempfile
from ase.io.elk import ElkReader

ElkCalculation = CalculationFactory("elk")


class ElkParser(Parser):
    """
    Parser class for parsing output of calculation.
    """

    def __init__(self, node):
        """
        Initialize Parser instance

        Checks that the ProcessNode being passed was produced by a ElkCalculation.

        :param node: ProcessNode of calculation
        :param type node: :class:`aiida.orm.nodes.process.process.ProcessNode`
        """
        super().__init__(node)
        if not issubclass(node.process_class, ElkCalculation):
            raise exceptions.ParsingError("Can only parse ElkCalculation")

    def parse(self, **kwargs):
        """
        Parse outputs, store results in database.

        :returns: an exit code, if parsing fails (or nothing if parsing succeeds)
        """
        output_filename = self.node.get_option("output_filename")

        # Check that folder content is as expected
        files_retrieved = self.retrieved.list_object_names()
        files_expected = [output_filename] + ElkCalculation._LIST_OF_OUTPUTS
        # Note: set(A) <= set(B) checks whether A is a subset of B
        if not set(files_expected) <= set(files_retrieved):
            self.logger.error(
                f"Found files '{files_retrieved}', expected to find '{files_expected}'"
            )
            return self.exit_codes.ERROR_MISSING_OUTPUT_FILES

        # add output file
        self.logger.info(f"Parsing '{output_filename}'")

        with tempfile.TemporaryDirectory() as tmp_path:
            self.retrieved.base.repository.copy_tree(tmp_path)
            er = ElkReader(tmp_path)
            output_params = Dict(dict=er.read_everything())

        self.out("output_parameters", output_params)

        return ExitCode(0)
