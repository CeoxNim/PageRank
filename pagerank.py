import pickle
import numpy as np
import matplotlib.pyplot as plt

root_path = "data/"
with open(root_path + "subGraph.pkl", "rb") as f_subGraph:
    graph = pickle.load(f_subGraph)
with open(root_path + "subConcept.pkl", "rb") as f_subConcept:
    r_concept_idx_final = pickle.load(f_subConcept)

# recreate index in subGraph
cnt = 0
id_map = {}
r_id_map = {}
for concept_id in graph:
    id_map[concept_id] = cnt 
    r_id_map[cnt] = r_concept_idx_final[concept_id]
    cnt += 1

# calculate pagerank - iteratively
iter = 1
d = 0.85
eps = 1e-3
length = cnt
pagerank = [[], []]
pagerank[0] = [1 for i in range(length)]

while True:
    curr_idx = iter & 1
    last_idx = 1 - curr_idx
    pagerank[curr_idx] = [(1 - d) for i in range(length)]
    for concept_id in graph:
        outlink_num = len(graph[concept_id])
        if outlink_num == 0: continue
        contribution = d * pagerank[last_idx][id_map[concept_id]] / outlink_num
        for outlink_id in graph[concept_id]:
            pagerank[curr_idx][id_map[outlink_id]] += contribution
    iter += 1

    delta = np.array([(pagerank[0][j] - pagerank[1][j]) for j in range(length)])
    delta = np.linalg.norm(delta, ord=2)
    print(f"Iter: {iter}, Delta: {delta}")
    if delta < eps: break

pagerank = pagerank[1 - (iter & 1)]

# Get the pagerank results and sort
results = {}
scores_sum = 0
for i, score in enumerate(pagerank):
    results.update({r_id_map[i]: score})
    scores_sum += score
results = sorted(results.items(), key=lambda x: x[1], reverse=True)

# Save the pagerank to .txt
with open(root_path + "pagerank_result.txt", "w", encoding="utf-8") as f_result:
    for t in results:
        f_result.write(t[0] + "\t" + str(t[1] / scores_sum) + "\n")








