from os import pathconf_names
import re
import gc
import pickle

class Args:
    origin_file = "enwiki-20211020-pages-articles-multistream.xml"


# Match the corresponding pattern string
class Patterns:
    page_end = re.compile(r"<\/page>")
    title = re.compile(r"<title>([^:]*)<\/title>")
    outlink = re.compile(r'\[\[[^:\[\]]*\]\]')
    is_redirect = re.compile(r"<redirect title=")

# store every 1e5 pieces
def store_block(block_id):
    
    global concept_graph, concept_idx, idx

    with open(f"data/concept_graph_block{block_id}.pkl", 'wb') as f_concept_graph:
        pickle.dump(concept_graph, f_concept_graph)
        concept_graph = {}
    print(f"{idx} pieces of concept_graph have been saved!")

    with open(f"data/concept_idx_block{block_id}.pkl", "wb") as f_concept_idx:
        pickle.dump(concept_idx, f_concept_idx)
        concept_idx = {}
    print(f"{idx} pieces of cencept_id have been saved")

    gc.collect()

# [cite1|cite2] -- only select cite1 (cite2 is "Category")
def do_outlinks_filter(outlinks):
    outlinks_filter = []
    for link in outlinks:
        link = link[2:-2]
        pos = link.find("|")
        if pos != -1:
            link = link[: pos]
        outlinks_filter.append(link)
    return outlinks_filter
            

concept = ""            # Concept per page
idx = 0                 # Id -- match concept
block_id = 0            # for save (every 1e5 pieces)
is_direct = False       # with direct title
is_store = True         # for store the block
link_list = []          # outlinks list per concept
concept_idx = {}        # {concept: id}
concept_graph = {}      # {concept: list(outlinks)}
    
with open(Args.origin_file, encoding="utf8") as origin_file:
    for line in origin_file:
        # find a new page (end)
        if Patterns.page_end.search(line):
            if not is_direct and concept:
                idx += 1
                is_store = False
                concept_idx[concept] = idx
                concept_graph[concept] = link_list
            concept = ""
            link_list = []
            is_direct = False
        
        # get the concept from the title
        if Patterns.title.search(line):
            concept = Patterns.title.search(line).group(1)
        
        # redirect page
        if Patterns.is_redirect.search(line):
            is_direct = True
        
        # get the outlinks from the page
        outlinks = Patterns.outlink.findall(line)
        outlinks = do_outlinks_filter(outlinks)
        link_list.extend(outlinks)

        # store avg 1e5 items
        if idx % 1e5 == 0 and is_store == False:
            is_store = True
            block_id += 1
            store_block(block_id)

store_block(block_id + 1)



