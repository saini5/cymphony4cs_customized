from django.utils import timezone
from django.conf import settings


class Run:
    """
        A class to represent a run.

        Attributes
        ----------
        id : int
            run id
        workflow_id : int
            parent workflow
        project_id : int
            workflow's parent project
        user_id : int
            project's owner
        name: str
            run name
        description : str
            Description of run
        status : str
            Status of run
        type: str
            Type of run
        date_creation : datetime.datetime
            time of run creation
        notification_url : str
            URL to send notifications to
    """
    def __init__(self, workflow_id, project_id, user_id, run_name, run_description, run_status=settings.RUN_STATUS[1], run_type=settings.RUN_TYPES[1], date_creation=timezone.now(), run_id=-1, notification_url=None):
        """
            Constructs all the necessary attributes for the run object.

            Parameters
            ----------
            id : int (optional)
                run id
            workflow_id : int
                parent workflow
            project_id : int
                workflow's parent project
            user_id : int
                project's owner
            name: str
                run name
            description : str
                Description of run
            status : str
                Status of run
            type: str
                Type of run
            date_creation : datetime.datetime (optional)
                time of run creation
            notification_url : str (optional)
                URL to send notifications to
        """
        self.id = run_id
        self.workflow_id = workflow_id
        self.project_id = project_id
        self.user_id = user_id
        self.name = run_name
        self.description = run_description
        self.status = run_status
        self.type = run_type
        self.date_creation = date_creation
        self.notification_url = notification_url

    def __str__(self):
        return 'Run({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9})'.format(
            self.id, self.workflow_id, self.project_id, self.user_id, self.name, self.description, self.status, self.type, self.date_creation, self.notification_url
        )


class Node:
    """
        A class to represent a node.

        Attributes
        ----------
        id : int
            node id
        name : str
            node name
        type : str
            node type
    """
    def __init__(self, node_id: int, node_name: str, node_type: str):
        self.id = node_id
        self.name = node_name
        self.type = node_type

    def __str__(self):
        return '(id: ' + str(self.id) + ', name: ' + self.name + ', type: ' + self.type + ')'


class Edge:
    """
        A class to represent an edge.

        Attributes
        ----------
        origin : Node
        destination: Node
    """
    def __init__(self, origin_node: Node, destination_node: Node):
        self.origin: Node = origin_node
        self.destination: Node = destination_node

    def __str__(self):
        return str(self.origin) + "->" + str(self.destination)


class DiGraph:
    """
        A class to represent a dag of nodes connected via edges
    """
    def __init__(self):
        self.nodes = set()
        self.edges = set()

    def add_node(self, node):
        self.nodes.add(node)

    def add_edge(self, edge):
        self.edges.add(edge)

    def search_node(self, node_name):
        # search for node with name
        for node in self.nodes:
            if node.name == node_name:
                return node
        return None

    def search_node_by_id(self, node_id):
        # search for node with id
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def __str__(self):
        return 'V: ' + str([str(node) for node in self.nodes]) + \
               '\n' + \
               'E: ' + str([str(edge) for edge in self.edges])

    def remove_edge(self, edge: Edge):
        self.edges.remove(edge)

    def get_incoming_nodes(self, node: Node):
        set_incoming_nodes = set()
        for edge in self.edges:
            origin: Node = edge.origin
            destination: Node = edge.destination
            if destination.id == node.id:
                set_incoming_nodes.add(origin)
        return set_incoming_nodes

    def get_outgoing_nodes(self, node: Node):
        set_outgoing_nodes = set()
        for edge in self.edges:
            origin: Node = edge.origin
            destination: Node = edge.destination
            if origin.id == node.id:
                set_outgoing_nodes.add(destination)
        return set_outgoing_nodes

    def get_edge(self, org: Node, dest: Node):
        for edge in self.edges:
            origin: Node = edge.origin
            destination: Node = edge.destination
            if origin == org and destination == dest:
                return edge
