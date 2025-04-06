import controller.logic.run.components as r

data_node_1 = r.Node(1, 'data1.csv', 'data')
operator_node_1 = r.Node(2, 'read_table', 'operator')
data_node_2 = r.Node(3, 'u_p_w_r_T', 'data')

edge_1 = r.Edge(data_node_1, operator_node_1)
edge_2 = r.Edge(operator_node_1, data_node_2)

run_dag = r.DiGraph()
run_dag.add_node(data_node_1)
run_dag.add_node(operator_node_1)
run_dag.add_node(data_node_2)
run_dag.add_edge(edge_1)
run_dag.add_edge(edge_2)
print(run_dag)