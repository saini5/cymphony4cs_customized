from django.db import connection
from django.utils import timezone
from django.conf import settings

import controller.logic.run.components as run_components
from controller.logic.common_data_access_operations import dict_fetchall, dict_fetchone
from controller.logic.common_logic_operations import get_run_prefix_table_name

import csv, re, shutil
from collections import OrderedDict
from pathlib import Path


def find_all_runs(workflow_id: int, project_id: int, user_id: int, run_type: str):
    """Return all runs of specific run type, under this user's project's workflow, from the db"""
    cursor = connection.cursor()
    list_all_runs = []
    try:
        # get all runs for this user>project>workflow from the all_runs table
        table_all_runs = "all_runs"
        cursor.execute(
            "SELECT r_id, w_id, u_id, p_id, r_name, r_desc, r_status, r_type, date_creation FROM " +
            table_all_runs +
            " WHERE w_id = %s AND p_id = %s AND u_id = %s AND r_type = %s",
            [workflow_id, project_id, user_id, run_type]
        )
        runs = dict_fetchall(cursor)
        for row in runs:
            obj_run = run_components.Run(
                run_id=row['r_id'],
                workflow_id=row['w_id'],
                project_id=row['p_id'],
                user_id=row['u_id'],
                run_name=row['r_name'],
                run_description=row['r_desc'],
                run_status = row['r_status'],
                run_type=row['r_type'],
                date_creation=row['date_creation']
            )
            list_all_runs.append(obj_run)
        return list_all_runs
    except ValueError as err:
        print('Data access exception in find all runs')
        print(err.args)
    finally:
        cursor.close()


def find_run(run_id: int, workflow_id: int, project_id: int, user_id: int):
    """Return the specified run from the db"""
    cursor = connection.cursor()
    run = None
    try:
        # get the specific run for this user project's workflow from the all_runs table
        table_all_runs = "all_runs"
        cursor.execute(
            "SELECT r_id, w_id, p_id, u_id, r_name, r_desc, r_status, r_type, date_creation FROM " +
            table_all_runs +
            " WHERE r_id = %s AND w_id = %s AND p_id = %s AND u_id = %s",
            [run_id, workflow_id, project_id, user_id]
        )
        run_row = dict_fetchone(cursor)
        obj_run = run_components.Run(
            run_id=run_row['r_id'],
            workflow_id=run_row['w_id'],
            project_id=run_row['p_id'],
            user_id=run_row['u_id'],
            run_name=run_row['r_name'],
            run_description=run_row['r_desc'],
            run_status=run_row['r_status'],
            run_type=run_row['r_type'],
            date_creation=run_row['date_creation']
        )
        run = obj_run
        return run
    except ValueError as err:
        print('Data access exception in find run')
        print(err.args)
    finally:
        cursor.close()


def create_run(obj_run: run_components.Run):
    """Insert the run into db"""
    run_id = None
    cursor = connection.cursor()
    try:
        # 1. insert run entry into all_runs table
        table_all_runs = "all_runs"
        cursor.execute(
            "INSERT into " +
            table_all_runs +
            " (w_id, p_id, u_id, r_name, r_desc, r_status, r_type, date_creation) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" +
            " RETURNING r_id;",
            [
                obj_run.workflow_id,
                obj_run.project_id,
                obj_run.user_id,
                obj_run.name,
                obj_run.description,
                obj_run.status,
                obj_run.type,
                timezone.now()  # store this time of creation
            ]
        )
        run_id = cursor.fetchone()[0]
        return run_id
    except ValueError as err:
        print('Data access exception in create run')
        print(err.args)
    finally:
        cursor.close()


def edit_run(obj_run: run_components.Run):
    """Edit the specified run in the db"""
    cursor = connection.cursor()
    try:
        # update job entry into all_jobs table
        table_all_runs = "all_runs"
        cursor.execute(
            "UPDATE " +
            table_all_runs +
            " SET " +
            "r_name = %s, r_desc = %s, r_status = %s" +
            " WHERE u_id = %s AND p_id = %s AND w_id = %s AND r_id = %s",
            [
                obj_run.name,
                obj_run.description,
                obj_run.status,
                obj_run.user_id,
                obj_run.project_id,
                obj_run.workflow_id,
                obj_run.id
            ]
        )
        return
    except ValueError as err:
        print('Data access exception in edit run')
        print(err.args)
    finally:
        cursor.close()


def copy_source_contents_to_target_dir(source_dir: Path, target_dir: Path):
    """Copy contents of the source directory to a target directory"""
    try:
        # create target dir
        target_dir.mkdir(parents=True, exist_ok=True)
        # iterate on source dir files
        if source_dir.is_dir():
            for source_file_path in source_dir.iterdir():
                if source_file_path.is_file():
                    # if a file with this name already exists in the target dir, it will be overwritten
                    shutil.copy(src=str(source_file_path), dst=str(target_dir))
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in copy workflow files to run directory')


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
        return obj_run_dag
    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in parse')


def parse_to_intermediate_representation(cy_file_path: Path):
    """
        Returns ordered dict mapping_line_tokens of line vs dict.
        mapping_line_tokens[line]['operator'] = operator    (a string)
        mapping_line_tokens[line]['arguments'] = arguments  (a list)
        mapping_line_tokens[line]['variables'] = variables  (a list)
    """
    mapping_line_tokens = OrderedDict()
    try:
        # patterns
        # operator pattern
        operator_patterns = []
        for operator in settings.BLACKBOX_OPERATORS:
            operator_pattern = str(operator) + '(.*)'
            operator_patterns.append(operator_pattern)
        # operator_patterns = ['read_table(.*)', '3a_kn(.*)', 'write_table(.*)']
        operator_regexes = [re.compile(pattern, re.DOTALL) for pattern in operator_patterns]
        # LHS pattern
        left_side_pattern = '.+?='  #+? makes it non-greedy
        left_side_regex = re.compile(left_side_pattern, re.DOTALL)
        # RHS patterns
        # arguments_pattern = '(.*)'
        arguments_pattern = '[(].*[)]'
        arguments_regex = re.compile(arguments_pattern, re.DOTALL)
        # split based on ( or , or )
        parenthesis_comma_pattern = '[(,)]'
        # lines of the program (sentence split)
        program = cy_file_path.read_text(encoding=settings.WORKFLOW_FILE_ENCODING)
        program = program.strip()
        # pattern to identify comments - c language style comments /*....*/
        comment_pattern = '/\*(.*?)\*/'
        comment_pattern_regex = re.compile(comment_pattern, re.DOTALL)
        # first, replace the comments inside the program by empty strings
        m = comment_pattern_regex.findall(program)
        if m:
            for i in range(len(m)):
                content = '/*' + m[i] + '*/'
                program = program.replace(content, '')
            # print(m)
        # pattern to identify list i.e. [...]
        list_pattern = '\[(.*?)\]'
        list_pattern_regex = re.compile(list_pattern, re.DOTALL)
        # second, pack the lists inside the program
        m = list_pattern_regex.findall(program)
        map_variable_vs_unpacked_list_contents = dict()
        if m:
            for i in range(len(m)):
                temp_variable = '_temp_listed_start_' + str(i) + '_end'
                map_variable_vs_unpacked_list_contents[temp_variable] = '[' + m[i] + ']'
            # print(m)
            # print(map_variable_vs_unpacked_list_contents)
        for variable, content in map_variable_vs_unpacked_list_contents.items():
            program = program.replace(content, variable)
        # pattern to identify double quotes i.e. "..."
        double_quoted_pattern = '\"(.*?)\"'
        double_quoted_pattern_regex = re.compile(double_quoted_pattern, re.DOTALL)
        # third, pack the string in double quotes inside the program
        m = double_quoted_pattern_regex.findall(program)
        map_variable_vs_unpacked_double_quoted_contents = dict()
        if m:
            for i in range(len(m)):
                temp_variable = '_temp_double_quoted_start_' + str(i) + '_end'
                map_variable_vs_unpacked_double_quoted_contents[temp_variable] = '"' + m[i] + '"'
            # print(m)
            # print(map_variable_vs_unpacked_double_quoted_contents)
        for variable, content in map_variable_vs_unpacked_double_quoted_contents.items():
            program = program.replace(content, variable)
        # assuming line ends in ;
        lines = program.split(settings.WORKFLOW_STATEMENT_TERMINATOR)
        # remove whitespace or new lines surrounding lines (these could have been formed when we removed comments above)
        lines = [x.strip() for x in lines]
        for line in lines:
            line = line.strip()
            if line == '':
                continue
            mapping_line_tokens[line] = {}
            for regex in operator_regexes:
                m = regex.search(line)
                if m:
                    match = m.group()
                    # extract operator
                    operator = match.split('(')[0]
                    # extract arguments
                    new_arguments = []
                    args_string = arguments_regex.search(match)
                    if args_string:
                        sub_match = args_string.group()
                        # print(sub_match)
                        # split the arguments
                        tokens = re.split(parenthesis_comma_pattern, sub_match)  # split based on ( or , or )
                        arguments = [token.strip() for token in tokens[1:-1]]
                        # print('normal arguments: ', arguments)
                        # unpack the list and quoted arguments
                        for arg in arguments:
                            new_arg = arg
                            for variable, content in map_variable_vs_unpacked_list_contents.items():
                                if variable in arg:
                                    new_arg = arg.replace(variable, content)
                                    break
                            for variable, content in map_variable_vs_unpacked_double_quoted_contents.items():
                                if variable in arg:
                                    new_arg = arg.replace(variable, content)
                                    break
                            new_arguments.append(new_arg)
                    mapping_line_tokens[line]['operator'] = operator
                    mapping_line_tokens[line]['arguments'] = new_arguments
                    # print('Operator: ', operator)
                    # print('Arguments: ', new_arguments)
                    break
            variables = None
            if mapping_line_tokens[line]['operator'] != settings.AUTOMATIC_OPERATORS[3]:    # operator not 'write_table'
                m = left_side_regex.match(line)
                if m:   # operator with LHS assignment
                    match = m.group().strip()
                    if match.startswith('('):
                        # split based on ( or , or )
                        tokens = re.split(parenthesis_comma_pattern, match)
                        variables = [token.strip() for token in tokens[1:-1]]
                    else:
                        variables = [match.replace('=', '').strip()]
                else:
                    raise ValueError('Should not have reached here in parsing, because operator not recognized')
            else:
                variables = [None]
            mapping_line_tokens[line]['variables'] = variables
            # print('Variables: ', variables)
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
                if operator != settings.AUTOMATIC_OPERATORS[0]:     # operator not 'read_table'
                    raise ValueError("Program should start with read_table operator")

            # 5. variables are defined before they are used
            for argument in arguments:

                # is the argument a string literal rather than variable
                if '"' in argument:
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
    """Check each operators semantics"""
    try:
        # naming convention
        naming_convention_pattern = settings.NAMING_CONVENTION_PATTERN
        naming_convention_regex = re.compile(naming_convention_pattern)
        for key, value in intermediate_program_representation.items():
            variables = value['variables']
            operator = value['operator']
            arguments = value['arguments']
            # 1. For read table
            if operator == settings.AUTOMATIC_OPERATORS[0]:     # 'read_table'
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
                if not (input_argument.startswith('"') and input_argument.endswith('"')):
                    raise ValueError("Input to read_table has to be a string literal")
                input_file_name = input_argument[1:-1]
                # check if extension is csv
                if not input_file_name.endswith(".csv"):
                    raise ValueError("Input argument to read_table does not represent a csv file")
                # Check if the csv file has been uploaded
                file_path = run_dir_path.joinpath(input_file_name)
                # print(file_path)
                if not file_path.is_file():
                    raise ValueError("read_table input file was not uploaded to file system")
                # open data file to record some basic stats
                with file_path.open() as data_file:
                    csv_reader = csv.reader(data_file)
                    headers = next(csv_reader)
                    row_count = sum(1 for row in csv_reader) # this will not include header row
                # Check header conventions and max limit
                for header in headers:
                    m = naming_convention_regex.match(header.strip())
                    if m is None:
                        raise ValueError("One or more header of input data file does not follow naming conventions")
                if len(headers) > settings.INPUT_N_MAX_HEADERS:
                    raise ValueError("Number of columns in data file exceeds the maximum allowed")
                # Check records limit
                if row_count > settings.INPUT_N_MAX_RECORDS:
                    raise ValueError("Number of records in data file exceeds the maximum allowed")
            elif operator == settings.AUTOMATIC_OPERATORS[3]:   # 'write_table'
                # variables
                variables = [x for x in variables if x is not None]
                # no variable should be the output
                if len(variables) > 0:
                    raise ValueError("No output variable allowed for write_table")
                # arguments
                # check if only two arguments
                if not ( len(arguments) == 2 ):
                    raise ValueError("Two arguments required for write_table")
                input_argument_1 = str(arguments[0]).strip()
                input_argument_2 = str(arguments[1]).strip()
                # input_argument_1 has to follow naming conventions
                m = naming_convention_regex.match(input_argument_1)
                if m is None:
                    raise ValueError("First input argument of write_table does not follow naming conventions")
                # the second argument has to be a string literal representing a file like write_table(x, file="xyz.csv")
                if not (input_argument_2.startswith('file') and input_argument_2.endswith('"')):
                    raise ValueError("Second input to write_table has to be a file represented by string literal")
            elif operator == settings.AUTOMATIC_OPERATORS[1]:   # 'sample_random'
                # variables
                # only one variable should be the output
                if not ( len(variables) == 1 ):
                    raise ValueError("Only one output variable allowed for sample_random")
                # arguments
                # check if only two arguments
                if not ( len(arguments) == 2 ):
                    raise ValueError("Two arguments required for sample_random")
                input_argument_1 = str(arguments[0]).strip()
                input_argument_2 = str(arguments[1]).strip()
                # input_argument_1 has to follow naming conventions
                m = naming_convention_regex.match(input_argument_1)
                if m is None:
                    raise ValueError("First input argument of sample_random does not follow naming conventions")
                # the second argument has to be a string literal representing number of records to sample like sample_random(x, n=2)
                if not ( input_argument_2.startswith('n') ):
                    raise ValueError("Second input to sample_random has to be of the form n = ")
            # 2. For operator number 2
            # elif another operator
        # all operators' semantics have been checked
        return
    except ValueError as err:
        print(err.args)
        raise ValueError('Semantic error')


def build_dag(intermediate_program_representation: OrderedDict):
    """Create the directed acyclic graph from program representation"""
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

            if operator != settings.AUTOMATIC_OPERATORS[3]:     # operator not 'write_table'
                input_nodes = []
                # for each input
                for argument in arguments:
                    existing_data_node = obj_run_dag.search_node(node_name=argument)
                    if existing_data_node:
                        input_nodes.append(existing_data_node)
                    else:
                        input_node = run_components.Node(i, argument, 'data')
                        i=i+1
                        input_nodes.append(input_node)

                output_nodes = []
                # for each output variable
                var_index = 0
                for variable in variables:
                    output_node = run_components.Node(i, variable, 'data:' + str(var_index))
                    i=i+1
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


def store_dag(obj_run: run_components.Run, obj_run_dag: run_components.DiGraph):
    """Store this dag in db"""

    cursor = connection.cursor()
    try:
        # create empty dag table
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_nodes = table_prefix + "nodes"
        table_edges = table_prefix + "edges"
        cursor.callproc('create_table_run_nodes',
                        [table_nodes])
        cursor.callproc('create_table_run_edges',
                        [table_edges])

        # store dag nodes in table_nodes
        for node in obj_run_dag.nodes:
            cursor.execute(
                "INSERT into " +
                table_nodes +
                " (n_id, n_name, n_type, date_creation) VALUES (%s, %s, %s, %s)",
                [
                    node.id,
                    node.name,
                    node.type,
                    timezone.now()
                ]
            )
        for edge in obj_run_dag.edges:
            origin_node: run_components.Node = edge.origin
            destination_node: run_components.Node = edge.destination
            cursor.execute(
                "INSERT into " +
                table_edges +
                " (o_id, d_id, date_creation) VALUES (%s, %s, %s)",
                [
                    origin_node.id,
                    destination_node.id,
                    timezone.now()
                ]
            )

        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store dag')

    finally:
        cursor.close()


def load_dag(obj_run: run_components.Run):
    """Load the run's dag from db"""

    cursor = connection.cursor()
    obj_run_dag: run_components.DiGraph = run_components.DiGraph()
    try:
        # create empty dag table
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_nodes = table_prefix + "nodes"
        table_edges = table_prefix + "edges"

        # load dag nodes from table_nodes, and add to the empty dag
        cursor.execute(
            "SELECT n_id, n_name, n_type, date_creation FROM " +
            table_nodes,
            []
        )
        node_rows = dict_fetchall(cursor)
        for node_row in node_rows:
            obj_node = run_components.Node(
                node_id=node_row['n_id'],
                node_name=node_row['n_name'],
                node_type=node_row['n_type']
            )
            obj_run_dag.add_node(obj_node)

        # load dag edges from table_edges, and add to the dag
        cursor.execute(
            "SELECT o_id, d_id, date_creation FROM " +
            table_edges,
            []
        )
        edge_rows = dict_fetchall(cursor)
        for edge_row in edge_rows:
            origin_node_id = edge_row['o_id']
            destination_node_id = edge_row['d_id']

            origin_node: run_components.Node = obj_run_dag.search_node_by_id(node_id=origin_node_id)
            destination_node: run_components.Node = obj_run_dag.search_node_by_id(node_id=destination_node_id)

            obj_edge = run_components.Edge(origin_node=origin_node, destination_node=destination_node)
            obj_run_dag.add_edge(obj_edge)

        return obj_run_dag

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in load dag')

    finally:
        cursor.close()


def store_mapping_node_vs_job(mapping_node_id_vs_job_id: dict, obj_run: run_components.Run):
    """Store this mapping in db"""

    cursor = connection.cursor()
    try:
        # create empty dag table
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_mapping = table_prefix + "mapping_operator_node_vs_job"
        cursor.callproc('create_table_run_mapping_operator_node_vs_job',
                        [table_mapping])

        # store dag nodes in table_nodes
        for key, value in mapping_node_id_vs_job_id.items():
            node_id = key
            job_id = value   # till run, ids are already there in the name of the table
            cursor.execute(
                "INSERT into " +
                table_mapping +
                " (n_id, j_id, date_creation) VALUES (%s, %s, %s)",
                [
                    node_id,
                    job_id,
                    timezone.now()
                ]
            )

        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store mapping of operator node vs job')

    finally:
        cursor.close()


def load_mapping_node_vs_job(obj_run: run_components.Run):
    """Load mapping of operator node vs job from db"""

    cursor = connection.cursor()
    mapping_node_id_vs_job_id = dict()       # node id vs job id
    try:
        # create empty dag table
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_mapping = table_prefix + "mapping_operator_node_vs_job"

        # load dag nodes from table_nodes, and add to the empty dag
        cursor.execute(
            "SELECT n_id, j_id, date_creation FROM " +
            table_mapping,
            []
        )
        rows_mapping_operator_node_id_vs_job_id = dict_fetchall(cursor)

        for mapping_row in rows_mapping_operator_node_id_vs_job_id:
            node_id = mapping_row['n_id']
            job_id = mapping_row['j_id']
            mapping_node_id_vs_job_id[node_id] = job_id

        return mapping_node_id_vs_job_id

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in load mapping node vs job')

    finally:
        cursor.close()


def store_execution_order(mapping_node_id_vs_position: dict, obj_run: run_components.Run):
    """Store this execution order in db"""
    # TODO:
    # 1. Human run (normal): store execution order using this, and load it while progressing dag
    # 2. Simulated run: store execution order using this, and load it whenever needed and while progressing dag

    cursor = connection.cursor()
    try:
        # create empty execution order table for nodes in the run dag
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_execution_order = table_prefix + "nodes_execution_order"
        cursor.callproc('create_table_run_nodes_execution_order',
                        [table_execution_order])

        # store dag nodes in table_nodes
        for key, value in mapping_node_id_vs_position.items():
            cursor.execute(
                "INSERT into " +
                table_execution_order +
                " (n_id, position, date_creation) VALUES (%s, %s, %s)",
                [
                    key,
                    value,
                    timezone.now()
                ]
            )

        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store execution order of nodes in run dag')

    finally:
        cursor.close()


def load_execution_order(obj_run: run_components.Run):
    """Load execution order of nodes of this run dag from db"""

    cursor = connection.cursor()
    mapping_node_id_vs_position = OrderedDict()       # node id vs execution position (0 means executed first)
    try:
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_execution_order = table_prefix + "nodes_execution_order"

        # load dag nodes from table_nodes, and add to the empty dag
        cursor.execute(
            "SELECT n_id, position, date_creation FROM " +
            table_execution_order +
            " ORDER BY position",
            []
        )
        rows_execution_order_mapping_node_id_vs_position = dict_fetchall(cursor)

        for mapping_row in rows_execution_order_mapping_node_id_vs_position:
            node_id = mapping_row['n_id']
            position = mapping_row['position']
            mapping_node_id_vs_position[node_id] = position

        return mapping_node_id_vs_position

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in load execution order: mapping node id vs position')

    finally:
        cursor.close()


def store_run_amt_credentials(amt_credentials: dict, obj_run: run_components.Run):
    """store the encrypted amt credentials in db i.e. store run level amt credentials for a run"""

    cursor = connection.cursor()
    try:
        # create empty amt credentials table
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_amt_credentials = table_prefix + "amt_credentials"
        cursor.callproc('create_table_run_amt_credentials',
                        [table_amt_credentials])

        # fill the table up
        for key, value in amt_credentials.items():
            cursor.execute(
                "INSERT into " + table_amt_credentials +
                " (key, value, value_data_type) VALUES (%s, %s, %s)",
                [key, value, str(type(value))]
            )

        return

    except ValueError as err:
        print(err.args)
        raise ValueError('Data access exception in store amt credentials of run')

    finally:
        cursor.close()


def load_run_amt_credentials(obj_run: run_components.Run):
    """Load the encrypted amt credentials from db"""
    cursor = connection.cursor()
    amt_credentials = dict()
    try:
        table_prefix = get_run_prefix_table_name(obj_run=obj_run)
        table_amt_credentials = table_prefix + "amt_credentials"
        cursor.execute(
            "SELECT key, value, value_data_type, date_creation FROM " +
            table_amt_credentials,
            []
        )
        rows = dict_fetchall(cursor)
        for row in rows:
            amt_credentials[row['key']] = row['value']

        return amt_credentials

    except ValueError as err:
        print('Data access exception in load amt credentials for the run')
        print(err.args)

    finally:
        cursor.close()
