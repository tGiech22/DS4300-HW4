"""Build the sample graph in Redis and print book recommendations for Spencer."""

from __future__ import annotations

from graph_api import GraphAPI, create_client


def build_sample_graph(graph: GraphAPI) -> None:
    graph.clear()

    graph.add_node("Emily", "Person")
    graph.add_node("Spencer", "Person")
    graph.add_node("Brendan", "Person")
    graph.add_node("Trevor", "Person")
    graph.add_node("Paxton", "Person")

    graph.add_node("Cosmos", "Book", {"title": "Cosmos", "price": 17.00})
    graph.add_node("Database Design", "Book", {"title": "Database Design", "price": 195.00})
    graph.add_node("The Life of Cronkite", "Book", {"title": "The Life of Cronkite", "price": 29.95})
    graph.add_node("DNA and you", "Book", {"title": "DNA and you", "price": 11.50})

    graph.add_edge("Emily", "Database Design", "bought")
    graph.add_edge("Spencer", "Cosmos", "bought")
    graph.add_edge("Spencer", "Database Design", "bought")
    graph.add_edge("Brendan", "Database Design", "bought")
    graph.add_edge("Brendan", "DNA and you", "bought")
    graph.add_edge("Trevor", "Cosmos", "bought")
    graph.add_edge("Trevor", "Database Design", "bought")
    graph.add_edge("Paxton", "Database Design", "bought")
    graph.add_edge("Paxton", "The Life of Cronkite", "bought")

    graph.add_edge("Emily", "Spencer", "knows")
    graph.add_edge("Spencer", "Emily", "knows")
    graph.add_edge("Spencer", "Brendan", "knows")


def main() -> None:
    client = create_client()
    graph = GraphAPI(client)
    build_sample_graph(graph)

    recommendations = graph.get_recommendations("Spencer")
    print("Recommendations for Spencer:")
    for title in recommendations:
        print(title)


if __name__ == "__main__":
    main()
