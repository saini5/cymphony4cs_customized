import csv
import re
import shutil
from collections import OrderedDict
from pathlib import Path
from django.conf import settings
import controller.logic.run.components as run_components


def parse(run_dir_path: Path):
    """Parse the cy file in this run directory and return dag"""

    obj_run_dag = None

    try:
        # get the cy file from the run directory
        cy_file_path = None
        for file_path in run_dir_path.iterdir():
            if file_path.is_file():
                if file_path.suffix == '.cy':
                    cy_file_path = file_path

        # all functions below lie in this dao file itself

        # parse the cy file to build an intermediate representation of the csl program
        intermediate_prog_rep = parse_to_intermediate_representation(cy_file_path)

        # check syntax of the csl program
        check_syntax_program(intermediate_program_representation=intermediate_prog_rep)

        # check semantics of individual operators in the csl program
        check_semantics_operators(
            intermediate_program_representation=intermediate_prog_rep,
            run_dir_path=run_dir_path
        )

        # build a full dag out of intermediate rep
        # build dag in memory in terms of run component
        obj_run_dag = build_dag(intermediate_prog_rep)

        print(obj_run_dag)

        return obj_run_dag

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in parse')


def parse_to_intermediate_representation(cy_file_path: Path):
    """
        returns ordered dict mapping_line_tokens of line vs dict.
        mapping_line_tokens[line]['operator'] = operator    (a string)
        mapping_line_tokens[line]['arguments'] = arguments  (a list)
        mapping_line_tokens[line]['variables'] = variables  (a list)
    """
    mapping_line_tokens = OrderedDict()
    try:
        # lines of the program (sentence split)
        program = cy_file_path.read_text()
        program = program.strip()

        # assuming line ends in ;
        lines = program.split(';')

        # patterns
        # operator pattern
        operator_patterns = []
        for operator in settings.BLACKBOX_OPERATORS:
            operator_pattern = str(operator) + '(.*)'
            operator_patterns.append(operator_pattern)
        # operator_patterns = ['read_table(.*)', '3a_kn(.*)', 'write_table(.*)']
        operator_regexes = [re.compile(pattern) for pattern in operator_patterns]

        # LHS pattern
        left_side_pattern = '.+?='  #+? makes it non-greedy
        left_side_regex = re.compile(left_side_pattern)

        # RHS pattern
        # split based on ( or , or )
        parenthesis_pattern = '[(,)]'

        for line in lines:
            line = line.strip()
            if line == '':
                continue
            mapping_line_tokens[line] = {}
            for regex in operator_regexes:
                m = regex.search(line)
                if m:
                    match = m.group()
                    # split based on ( or , or )
                    tokens = re.split(parenthesis_pattern, match)
                    operator = tokens[0]
                    arguments = [token.strip() for token in tokens[1:-1]]
                    mapping_line_tokens[line]['operator'] = operator
                    mapping_line_tokens[line]['arguments'] = arguments
                    print(operator)
                    print(arguments)
                    break

            m = left_side_regex.match(line)
            if m:   # write line won't match
                match = m.group().strip()
                if match.startswith('('):
                    # split based on ( or , or )
                    tokens = re.split(parenthesis_pattern, match)
                    variables = [token.strip() for token in tokens[1:-1]]
                else:
                    variables = [match.replace('=', '').strip()]
            else:
                variables = ['na']
            mapping_line_tokens[line]['variables'] = variables
            print(variables)

        return mapping_line_tokens
    except ValueError as err:
        print(err.args)
        raise ValueError('Parsing error')


def check_syntax_program(intermediate_program_representation: OrderedDict):
    """
    Check syntax of the entire csl program as a whole without digging into the individual operators
    :param intermediate_program_representation:
    :return: void
    """
    try:
        i = 0
        for key, value in intermediate_program_representation.items():
            variables = value['variables']
            operator = value['operator']
            arguments = value['arguments']

            # 1. each command ends in ;
            # sort of already checked for this while parsing in previous function

            # 2. each operator is of the form [moo1, moo2, ...] = op(bar, ...) or moo = op(bar, ...) or op(bar, ...)
            # sort of already checked for this while parsing in previous function

            # 3. op in each command is a blackbox operator
            if operator not in settings.BLACKBOX_OPERATORS:
                raise ValueError("Operator not recognized")

            # 4. start with at least one read_table operator
            if i == 0:
                if operator != 'read_table':
                    raise ValueError("Program should start with read_table operator")

            # 5. variables are defined before they are used
            for argument in arguments:

                # is the argument a string literal rather than variable
                if "'" in argument:
                    continue    # jump to next argument

                # is the argument a key-value pair
                if "=" in argument:
                    continue

                argument_defined_earlier = False
                # lookup in the variables defined upto now
                j = 0
                for key2,value2 in intermediate_program_representation.items():
                    if j < i:
                        # if argument is found in previously defined variables
                        if argument in value2['variables']:
                            argument_defined_earlier = True
                            break
                    j = j + 1

                # all search exhausted
                # argument is not a string literal but also not found in a pre-defined variable
                if not argument_defined_earlier:
                    raise ValueError("Variable used without defining first")

            i = i + 1
        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Syntax error')


def check_semantics_operators(intermediate_program_representation: OrderedDict, run_dir_path: Path):
    try:
        for key, value in intermediate_program_representation.items():
            variables = value['variables']
            operator = value['operator']
            arguments = value['arguments']
            # 1. For read table
            if operator == 'read_table':
                # naming convention
                naming_convention_pattern = '[a-zA-Z][a-zA-Z0-9_]*'
                naming_convention_regex = re.compile(naming_convention_pattern)

                # variables

                # only one variable should be the output
                if len(variables) > 1:
                    raise ValueError("Only one output variable allowed for read_table")
                output_variable = variables[0]

                # variable should follow naming convention
                m = naming_convention_regex.match(output_variable)
                if m is None:
                    raise ValueError("Output variable of read_table does not follow naming conventions")

                # arguments

                # check if only one argument
                if len(arguments) > 1:
                    raise ValueError("Only one argument allowed for read_table")
                input_argument = str(arguments[0])

                # the read_table argument has to be a string literal
                if not (input_argument.startswith("'") and input_argument.endswith("'")):
                    raise ValueError("Input to read_table has to be a string literal")

                input_file_name = input_argument[1:-1]
                # check if extension is csv
                if not input_file_name.endswith(".csv"):
                    raise ValueError("Input argument to read_table does not represent a csv file")

                # Check if the csv file has been uploaded
                file_path = run_dir_path.joinpath(input_file_name)
                print(file_path)
                if not file_path.is_file():
                    raise ValueError("read_table input file was not uploaded to file system")

                # open data file to record some basic stats
                with file_path.open() as data_file:
                    csv_reader = csv.reader(data_file)
                    headers = next(csv_reader)
                    row_count = sum(1 for row in csv_reader) # this will not include header row

                # Check header conventions and max limit
                for header in headers:
                    m = naming_convention_regex.match(header)
                    if m is None:
                        raise ValueError("One or more header of input data file does not follow naming conventions")

                if len(headers) > settings.INPUT_N_MAX_HEADERS:
                    raise ValueError("Number of columns in data file exceeds the maximum allowed")

                # Check records limit
                if row_count > settings.INPUT_N_MAX_RECORDS:
                    raise ValueError("Number of records in data file exceeds the maximum allowed")

            # 2. For operator number 2
            # elif another operator

        # all operators' semantics have been checked
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Semantic error')


def build_dag(intermediate_program_representation: OrderedDict):
    obj_run_dag = run_components.DiGraph()
    try:
        i = 0
        # for each line
        for key, value in intermediate_program_representation.items():
            variables = value['variables']
            operator = value['operator']
            arguments = value['arguments']

            # create operator node
            operator_node = run_components.Node(i, operator, 'operator')
            i = i + 1

            if operator != 'write_table':
                input_nodes = []
                # for each input
                for argument in arguments:
                    existing_data_node = obj_run_dag.search_node(node_name=argument)
                    if existing_data_node:
                        input_nodes.append(existing_data_node)
                    else:
                        input_node = run_components.Node(i, argument, 'data')
                        i = i + 1
                        input_nodes.append(input_node)

                output_nodes = []
                # for each output variable
                var_index = 0
                for variable in variables:
                    output_node = run_components.Node(i, variable, 'data:' + str(var_index))
                    i = i + 1
                    output_nodes.append(output_node)
                    var_index = var_index + 1
            else:
                input_nodes = []
                output_nodes = []
                # first input is the input node
                existing_data_node = obj_run_dag.search_node(node_name=arguments[0])
                input_nodes.append(existing_data_node)
                # second input is the output node
                output_node = run_components.Node(i, arguments[1], 'data:' + 'na')
                i = i + 1
                output_nodes.append(output_node)

            # put everything together
            obj_run_dag.add_node(operator_node)
            for input_node in input_nodes:
                obj_run_dag.add_node(input_node)
                input_edge = run_components.Edge(input_node, operator_node)
                obj_run_dag.add_edge(input_edge)

            for output_node in output_nodes:
                obj_run_dag.add_node(output_node)
                output_edge = run_components.Edge(operator_node, output_node)
                obj_run_dag.add_edge(output_edge)

        return obj_run_dag
    except ValueError as err:
        print(err.args)
        raise ValueError('Error while building dag')


dir_path = Path('./cymphony').joinpath(
            'u5',
            'p4',
            'w3',
            'r17'
        )

run_dag = parse(dir_path)

L = run_dag.get_execution_order()
print([str(i) for i in L])