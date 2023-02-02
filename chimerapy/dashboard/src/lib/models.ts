// The data models

export interface Worker {

}

export interface Node {
    name: string;
    id: string;
}

export interface Graph {

}


export interface Manager {
    ip: string;  // Define proper IP
    port: number;
    workers: Worker[];
    nodes: Node[];
    graph: Graph;
    worker_graph_map: {string: string}[];
}
