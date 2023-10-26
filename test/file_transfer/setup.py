from setuptools import setup, find_packages

setup(
    name="ft",
    version="0.0.1",
    packages=find_packages(),
    install_requires=[
        "chimerapy-engine",
        "chimerapy-orchestrator"
    ],
    entry_points={
        "chimerapy.orchestrator.nodes_registry": [
            "get_nodes_registry = ft:register_nodes_metadata"
        ]
    }
)