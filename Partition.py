import numpy as np
import networkx as nx


def partition_memory(layer_info, branch_order, start, end):
    memory = 0
    for i in range(start, end):
        layer_name = branch_order[i]
        memory += layer_info[layer_name]['memory']
    return memory

def partition_computation(layer_info, branch_order, device, start, end):
    computation = 0
    for i in range(start, end):
        layer_name = branch_order[i]
        layer = layer_info[layer_name]
        computation += (layer['computation'][device.id] * layer['input data size'])
    return computation

def divide_branchs(graph, node, checked_nodes=None):
    # DAG branch to different modules
    result_nodes = list()
    if checked_nodes is None:  # initialize
        checked_nodes = list()

    while True:
        if not result_nodes:
            result_nodes.append(node)
            checked_nodes.append(node)
        successors = list(graph.successors(node))
        num_successors = len(successors)
    
        if num_successors > 1:  # branches
            result_subgraph = [graph.subgraph(result_nodes)]
            for child in successors:
                if child not in checked_nodes:
                    result_subgraph += divide_branchs(graph, child, checked_nodes)
            return result_subgraph
        elif num_successors == 1:
            node = successors[0]
            num_pred = predecessors_num(graph, node)
            if num_pred == 1:
                result_nodes.append(node)
                checked_nodes.append(node)
            else:
                result_subgraph = [graph.subgraph(result_nodes)]
                if node not in checked_nodes:
                    result_subgraph += divide_branchs(graph, node, checked_nodes)
                return result_subgraph

        else:   # end
            return [graph.subgraph(result_nodes)]

def predecessors_num(graph, node):
    return len(list(graph.predecessors(node)))

def graph_ordering(graph):
    return list(nx.topological_sort(graph))


def partitioning(dag_graph, device_info, layer_info, time_constraint):
    dag_order_lst = graph_ordering(dag_graph)
    modules = divide_branchs(dag_graph, dag_order_lst[0])
    result = dict()
    for m in modules:
        branch_order = graph_ordering(m)
        start = 0
        cur = start + 1
        ex_util = 0.

        while cur < len(m):
            new_util = device_util(device_info, layer_info, time_constraint, branch_order, start, cur)
            if ex_util > new_util:
                result[branch_order[start]] = branch_order[start:cur]
                start = cur
                cur = start + 1
                ex_util = 0
            else:
                cur += 1
                ex_util = new_util

        if start < len(m):
            result[branch_order[start]] = branch_order[start:cur]
    return result


def device_util(device_info, layer_info, time_constraint, branch_order, start, end):
    usable_device_num = 0
    for device in device_info:
        if partition_computation(layer_info, branch_order, device, start, end) / device.computing_capacity <= time_constraint \
             and partition_memory(layer_info, branch_order, start, end) <= device.memory:
             usable_device_num += 1
    return usable_device_num



