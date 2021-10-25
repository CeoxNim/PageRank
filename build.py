import gc
import pickle
import random
from glob import glob
from tqdm import tqdm

# Load the concept_idx and get the r_concept_idx
# r_concept_idx: {id: concept}
def get_concept_idx(root_path):
    concept_idx = {}
    r_concept_idx = {}
    for filename in glob(root_path + "concept_idx_block*.pkl"):
        with open(filename, "rb") as f_concept_idx:
            word_idx_tmp = pickle.load(f_concept_idx)
        concept_idx.update(word_idx_tmp)
    for concept in concept_idx:
        r_concept_idx.update({concept_idx[concept]: concept})
    print("Get Concept Idx Finished!")
    return concept_idx, r_concept_idx

# Load the graph and get the graph_idx
# graph_idx: {id: list(outlink_idx)}
def get_graph_idx(root_path, pkl_num):
    graph_idx = {}
    for i in tqdm(range(1, pkl_num + 1)):
        graph = {}
        gc.collect()
        with open(root_path + f"concept_graph_block{i}.pkl", "rb") as f_concept_graph:
            graph = pickle.load(f_concept_graph)
        for word in graph:
            graph_idx_tmp = []
            for outlink in graph[word]:
                if len(outlink) == 0:
                    continue
                outlink_title = outlink[0].upper() + outlink[1: ]
                if outlink_title in concept_idx:
                    graph_idx_tmp.append(concept_idx[outlink_title])
            graph_idx.update({concept_idx[word]: graph_idx_tmp})
    print("Get Graph Idx Finished!")
    return graph_idx

# Sample the 150w nodes and del the outlink not in samples
def get_sample_with_node_del(n):
    buffer = []
    graph_idx_final = {}
    r_concept_idx_final = {}
    tot = len(graph_idx)
    flag = False
    while True:
        buffer.append(random.randint(1, tot + 1))
        while len(buffer) > 0:
            title_id = buffer.pop()
            if title_id not in graph_idx_final:
                graph_idx_final[title_id] = graph_idx[title_id]
                r_concept_idx_final[title_id] = r_concept_idx[title_id]
                buffer = graph_idx[title_id]
                if len(graph_idx_final) % 10000 == 0:
                    print(f"{len(graph_idx_final)} nodes selected!")
            if len(graph_idx_final) == n:
                flag = True
                break
        if flag: break
    for title_id in graph_idx_final:
        del_list = []
        for outlink_id in graph_idx_final[title_id]:
            if outlink_id not in graph_idx_final:
                del_list.append(outlink_id)
        for outlink_id in del_list:
            graph_idx_final[title_id].remove(outlink_id)
    return r_concept_idx_final, graph_idx_final

# save the sample nodes
def save_final(root_path):
    with open(root_path + "subGraph.pkl", "wb") as f_subGraph:
        pickle.dump(graph_idx_final, f_subGraph)
    with open(root_path + "subConcept.pkl", "wb") as f_subConcept:
        pickle.dump(r_concept_idx_final, f_subConcept)
    print("FINISHED!")

root_path = "data/"
pkl_num = 64

# like {"cxn": 1}   like {1: "cxn"}
concept_idx, r_concept_idx = get_concept_idx(root_path)

# graph just id like {1: [2, 3, 4]}
graph_idx = get_graph_idx(root_path, pkl_num)

print("Concept_idx_len: ", len(concept_idx))
print("Graph_idx_len: ", len(graph_idx))

# like {1: "cxn"}     like {1: [2, 3, 4]}
r_concept_idx_final, graph_idx_final = get_sample_with_node_del(1500000)
save_final(root_path)
