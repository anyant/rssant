import os

if os.getenv('MODULE_GRAPH_HOOKER') in ('1', 'true', 'True'):
    import module_graph
    module_graph.setup_hooker(save_to='data/rssant_worker_module_graph.json', verbose=True)

from rssant_common.actor_helper import start_actor  # noqa: F402


if __name__ == "__main__":
    start_actor('worker', port=6792)
