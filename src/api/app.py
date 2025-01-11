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

@app.route('/longest-path', methods=['GET'])
def longest_path():
    try:
        source = request.args.get('source')
        target = request.args.get('target')

        if not source or not target:
            return jsonify({"error": "Both 'source' and 'target' parameters are required."}), 400

        query = """
            MATCH path = (source:Word {word: $source})-[:RELATED_TO*]-(target:Word {word: $target})
            WHERE ALL(n IN nodes(path) WHERE EXISTS((n)-[:RELATED_TO]-()))
              AND NONE(n IN nodes(path)[1..-1] WHERE n = source OR n = target)
            RETURN [node IN nodes(path) | node.word] AS nodes, 
                   [rel IN relationships(path) | rel.weight] AS weights,
                   reduce(totalWeight = 0, r IN relationships(path) | totalWeight + r.weight) AS total_weight
            ORDER BY total_weight DESC
            LIMIT 1
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

@app.route('/clusters', methods=['GET'])
def clusters():
    try:
        cluster_query = """
            MATCH (n:Word)-[:RELATED_TO*]-(m:Word)
            WITH id(n) AS cluster_id, n.word AS root_word, collect(DISTINCT m.word) AS connected_words
            RETURN cluster_id, [root_word] + connected_words AS cluster_nodes
        """

        isolated_nodes_query = """
            MATCH (n:Word)
            WHERE NOT (n)-[]-()
            RETURN n.word AS word
        """

        with neo4j_connection.driver.session() as session:
            cluster_results = session.run(cluster_query)
            clusters = [
                {"component_id": record["cluster_id"], "nodes": record["cluster_nodes"]}
                for record in cluster_results
            ]

            isolated_results = session.run(isolated_nodes_query)
            isolated_nodes = [record["word"] for record in isolated_results]

        for isolated_node in isolated_nodes:
            clusters.append({"component_id": None, "nodes": [isolated_node]})

        if not clusters:
            return jsonify({"message": "No clusters found."}), 200

        return jsonify({"clusters": clusters})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/high-degree-nodes', methods=['GET'])
def high_degree_nodes():
    try:
        query = """
            MATCH (n:Word)-[r]-()
            WITH n, count(r) AS degree
            WHERE degree >= 8
            RETURN n.word AS word, degree
            ORDER BY degree DESC
        """

        with neo4j_connection.driver.session() as session:
            results = session.run(query)

            high_degree_nodes = [
                {"word": record["word"], "degree": record["degree"]}
                for record in results
            ]

        if not high_degree_nodes:
            return jsonify({"message": "No nodes found with high degree of connectivity."}), 200

        return jsonify({"high_degree_nodes": high_degree_nodes})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/nodes-by-degree/<int:degree>', methods=['GET'])
def nodes_by_degree(degree):
    try:
        query = """
            MATCH (n:Word)-[r]-()
            WITH n, count(r) AS degree
            WHERE degree = $degree
            RETURN n.word AS word, degree
            ORDER BY word
        """

        with neo4j_connection.driver.session() as session:
            results = session.run(query, degree=degree)

            nodes = [
                {"word": record["word"], "degree": record["degree"]}
                for record in results
            ]

        if not nodes:
            return jsonify({"message": f"No nodes found with degree {degree}."}), 200

        return jsonify({"nodes": nodes})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
