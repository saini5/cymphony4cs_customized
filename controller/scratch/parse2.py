from pathlib import Path
from django.conf import settings
import re
import csv
from collections import OrderedDict


def parse(run_dir_path: Path):
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
        # obj_run_dag = build_dag(intermediate_prog_rep)

        # if parse_and_check_syntax(cy_file_path):    # syntax of csl program as a whole
        #     if parse_and_check_semantics(run_dir_path): # semantics of each operator
        #         obj_run_dag = parse_and_build_dag(cy_file_path)   # build dag in memory in terms of run component
        #     else:
        #         print("semantics not valid")
        # else:
        #     print("syntax not valid")

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
        print('========In parse to intermediate rep========')
        # lines of the program (sentence split)
        program = cy_file_path.read_text()
        program = program.strip()

        # assuming line ends in ;
        lines = program.split(';')

        # patterns
        # operator pattern
        operator_patterns = ['read_table(.*)', '3a_kn(.*)', 'write_table(.*)']
        operator_regexes = [re.compile(pattern) for pattern in operator_patterns]

        # LHS pattern
        left_side_pattern = '.+='
        left_side_regex = re.compile(left_side_pattern)

        # RHS pattern
        # split based on ( or , or )
        parenthesis_pattern = '[(,)]'

        for line in lines:
            line = line.strip()
            if line == '':
                continue
            print(line)
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
                    print(operator, ' ', arguments, ' ', 'Match found: ', match)
                    break

            m = left_side_regex.match(line)
            if m:  # write line won't match
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
    try:
        print('========In check syntax of program rep========')
        i = 0
        for key, value in intermediate_program_representation.items():
            variables = value['variables']
            operator = value['operator']
            arguments = value['arguments']
            print(variables)
            print(operator)
            print(arguments)

            # each operator is of the form [moo1, moo2, ...] = op(bar, ...) or moo = op(bar, ...) or op(bar, ...)
            # sort of already checked for this (more like understood it, but not checked)

            # op in each command is a blackbox operator
            if operator not in ['read_table', 'write_table', '3a_kn']:
                raise ValueError("Operator not recognized")

            # start with at least one read_table operator
            if i == 0:
                if operator != 'read_table':
                    raise ValueError("Program should start with read_table operator")

            # variables defined before used
            for argument in arguments:

                # is the argument a string literal rather than variable
                if "'" in argument:
                    print('Literal: ', argument)
                    continue  # jump to next argument

                argument_defined_earlier = False
                # lookup in the variables defined upto now
                j = 0
                for key2, value2 in intermediate_program_representation.items():
                    if j < i:
                        # if argument is found in previously defined variables
                        if argument in value2['variables']:
                            argument_defined_earlier = True
                            print(argument_defined_earlier)
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
        print('========In check semantics of program rep against run dir========')
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

                print(input_argument)
                # the read_table argument has to be a string literal
                if not (input_argument.startswith("'") and input_argument.endswith("'")):
                    raise ValueError("Input to read_table has to be a string literal")

                input_file_name = input_argument[1:-1]
                # check if extension is csv
                print(input_file_name)
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
                    row_count = sum(1 for row in csv_reader)  # this will not include header row

                # Check header conventions and max limit
                print(headers)
                print(row_count)
                for header in headers:
                    print(header)
                    m = naming_convention_regex.match(header)
                    if m is None:
                        raise ValueError("One or more header of input data file does not follow naming conventions")

                if len(headers) > 100:
                    raise ValueError("Number of columns in data file exceeds the maximum allowed")

                # Check records limit
                if row_count > 500000:
                    raise ValueError("Number of records in data file exceeds the maximum allowed")

            # 2. For operator number 2
            # elif another operator

        # all operators' semantics have been checked
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Semantic error')


dir_path = Path('./cymphony').joinpath(
            'u5',
            'p4',
            'w2',
            'r13'
        )

parse(dir_path)