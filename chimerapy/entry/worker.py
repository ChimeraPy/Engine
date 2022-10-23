# Built-in Imports
import argparse


def main():

    # Internal Imports
    from chimerapy.worker import Worker

    # Create the arguments for Worker CI
    parser = argparse.ArgumentParser(description="ChimeraPy Worker CI")

    # Adding the arguments
    parser.add_argument("--name", type=str, help="Name of the Worker", required=True)
    parser.add_argument("--ip", type=str, help="Manager's IP Address", required=True)
    parser.add_argument("--port", type=int, help="Manager's Port", required=True)
    parser.add_argument(
        "--max_nodes",
        type=int,
        help="Maximum number of Nodes that Worker should support",
        default=10,
    )

    args = parser.parse_args()

    # Convert the Namespace to a dictionary
    d_args = vars(args)

    # Create Worker and execute connect
    worker = Worker(name=d_args["name"], max_num_of_nodes=d_args["max_nodes"])
    worker.connect(host=d_args["ip"], port=d_args["port"])


if __name__ == "__main__":
    main()
