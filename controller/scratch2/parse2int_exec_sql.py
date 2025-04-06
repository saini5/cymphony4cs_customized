import csv
import re
import shutil
from collections import OrderedDict
from pathlib import Path
from django.conf import settings


list_pattern = '\[(.*?)\]'
list_pattern_regex = re.compile(list_pattern)

mapping_line_tokens = OrderedDict()
# lines of the program (sentence split)
program = "T=read_table('inputt.csv'); " \
          "(y1) = exec_sql(T, queries=[“create table temp as (select * from T)”], temp=y1);" \
          "(y1) = exec_sql(T, queries=[create table temp as (select * from T) ; create table temp2 as (select * from T where _id = 3 and _uid = 32)], temp=y1, temp = y2);" \
          "(y3) = exec_sql(T, queries=[create table temp3 as (select orders.order_id, customers.customer_name from y1 INNER JOIN y2 ON y1.customer_id=y2.customer_id)], temp3=y3);" \
          "(y4) = exec_sql(T, queries=[create table temp4 as (select * from T) ; alter table temp4 drop column label; ], temp4=y4);" \
          "(B_1, C_1) = 3a_kn(T, 'instructions.html', k=2, n=3, question='Question in first 3a_kn', answers=['yes' or 'no'], annotation_time_limit=3600);"
program = program.strip()


m = list_pattern_regex.findall(program)
map_variable_vs_unpacked = dict()
if m:
  for i in range(len(m)):
    temp_variable = "_temp_" + str(i)
    map_variable_vs_unpacked[temp_variable] = m[i]
  print(m)
  print(map_variable_vs_unpacked)

for variable, content in map_variable_vs_unpacked.items():
    program = program.replace(content, variable)

# assuming line ends in ;
lines = program.split(';')
print(lines)

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

# arguments_pattern = '(.*)'
arguments_pattern = '[(].*[)]'
arguments_regex = re.compile(arguments_pattern)

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
            # tokens = re.split(parenthesis_pattern, match)

            # extract operator
            print(match)
            operator = match.split('(')[0]
            print('operator: ', operator)

            # text = "exec_sql(T, 'instruction.txt', queries=['create table temp as (select * from T)'], y1=temp);"
            # text = "3a_kn(T, 'instructions.html', k=2, n=3, question='Question in first 3a_kn', answers=['yes' or 'no'], annotation_time_limit=3600);"
            # text = "D_1 = sample_random(C_1, n=2);"

            new_arguments = []

            arguments_pattern = '[(].*[)]'
            arguments_regex = re.compile(arguments_pattern)

            args_string = arguments_regex.search(match)
            if args_string:
                sub_match = args_string.group()
                print(sub_match)

                # list_arg = ',.*?\[(.*?)\].*?,'
                # list_arg = '(?<=,)(.*?)(?=,)'
                # string_arg = '(?<=\')(.*?)(?=\')'

                # pack the list in a _temp variable
                # list_pattern = '\[(.*?)\]'
                # list_pattern_regex = re.compile(list_pattern)
                # m1 = list_pattern_regex.search(sub_match)
                # list_match = None
                # if m1:
                #     list_match = m1.group()
                #     print('list_match: ', list_match)
                #     sub_match = sub_match.replace(list_match, "_temp")
                #     print(sub_match)

                # break down the args now
                parenthesis_comma_pattern = '[(,)]'
                tokens = re.split(parenthesis_comma_pattern, sub_match)  # split based on ( or , or )
                arguments = [token.strip() for token in tokens[1:-1]]
                print('normal arguments: ', arguments)

                for arg in arguments:
                    print(arg)
                    new_arg = arg
                    for variable, content in map_variable_vs_unpacked.items():
                        if variable in arg:
                            print('inside if')
                            print(variable)
                            print(arg)
                            new_arg = arg.replace(variable, content)
                            break
                    new_arguments.append(new_arg)
                print(new_arguments)

            # operator = tokens[0]
            # arguments = [token.strip() for token in tokens[1:-1]]
            mapping_line_tokens[line]['operator'] = operator
            mapping_line_tokens[line]['arguments'] = new_arguments
            print('Operator: ', operator)
            print('Arguments: ', new_arguments)
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
    print('Variables: ', variables)



#########################
intermediate_program_representation = mapping_line_tokens
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
