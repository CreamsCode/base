from flask import Flask, request, jsonify
from src.graph.neo4j_connection import Neo4JConnection

app = Flask(__name__)

neo4j_connection = Neo4JConnection(uri="bolt://localhost:7687", username="neo4j", password="password")

@app.route('/shortest-path', methods=['GET'])
def shortest_path():
    try:
        source = request.args.get('source')
        target = request.args.get('target')

        if not source or not target:
            return jsonify({"error": "Both 'source' and 'target' parameters are required."}), 400

        query = """
            MATCH path = shortestPath((source:Word {word: $source})-[*]-(target:Word {word: $target}))
            RETURN [node IN nodes(path) | node.word] AS nodes, 
                   [rel IN relationships(path) | rel.weight] AS weights,
                   reduce(totalWeight = 0, r IN relationships(path) | totalWeight + r.weight) AS total_weight
        """

        with neo4j_connection.driver.session() as session:
            result = session.run(query, source=source, target=target).single()

        if not result:
            return jsonify({"error": "No path found between the specified nodes."}), 404

        return jsonify({
            "nodes": result["nodes"],
            "weights": result["weights"],
            "total_weight": result["total_weight"]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/all-paths', methods=['GET'])
def all_paths():
    try:
        source = request.args.get('source')
        target = request.args.get('target')

        if not source or not target:
            return jsonify({"error": "Both 'source' and 'target' parameters are required."}), 400

        query = """
            MATCH path = (source:Word {word: $source})-[*]-(target:Word {word: $target})
            RETURN nodes(path) AS nodes, relationships(path) AS relationships, 
                   reduce(total = 0, r IN relationships(path) | total + r.weight) AS total_weight
        """

        with neo4j_connection.driver.session() as session:
            result = session.run(query, source=source, target=target)

            paths = [
                {
                    "nodes": [node["word"] for node in record["nodes"]],
                    "weights": [rel["weight"] for rel in record["relationships"]],
                    "total_weight": record["total_weight"]
                }
                for record in result
            ]

        if not paths:
            return jsonify({"error": "No paths found between the specified nodes."}), 404

        return jsonify({"all_paths": paths})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/isolated-nodes', methods=['GET'])
def isolated_nodes():
    try:
        query = """
            MATCH (n:Word)
            WHERE NOT (n)-[]-()
            RETURN n.word AS word
        """

        with neo4j_connection.driver.session() as session:
            result = session.run(query)

            isolated_nodes = [record["word"] for record in result]

        if not isolated_nodes:
            return jsonify({"message": "No isolated nodes found."}), 200

        return jsonify({"isolated_nodes": isolated_nodes})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    
if __name__ == '__main__':
    app.run(debug=True)
