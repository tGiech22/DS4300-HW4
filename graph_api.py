"""Redis-backed graph storage for the DS4300 graph assignment."""

from __future__ import annotations

import json
from typing import Any

import redis


class GraphAPI:
    """Store nodes and typed edges in Redis using hashes and sets."""

    def __init__(self, client: redis.Redis, namespace: str = "graph") -> None:
        self.client = client
        self.ns = namespace

    def _node_key(self, name: str) -> str:
        return f"{self.ns}:node:{name}"

    def _all_nodes_key(self) -> str:
        return f"{self.ns}:nodes"

    def _type_nodes_key(self, node_type: str) -> str:
        return f"{self.ns}:nodes:type:{node_type}"

    def _adjacent_key(self, name: str) -> str:
        return f"{self.ns}:adjacent:{name}"

    def _edge_adjacent_key(self, name: str, edge_type: str) -> str:
        return f"{self.ns}:adjacent:{name}:edge:{edge_type}"

    def clear(self) -> None:
        """Remove all keys under this graph namespace."""
        pattern = f"{self.ns}:*"
        cursor = 0
        while True:
            cursor, keys = self.client.scan(cursor=cursor, match=pattern, count=100)
            if keys:
                self.client.delete(*keys)
            if cursor == 0:
                break

    def add_node(self, name: str, node_type: str, properties: dict[str, Any] | None = None) -> None:
        payload = {
            "name": name,
            "type": node_type,
            "properties": json.dumps(properties or {}, sort_keys=True),
        }
        with self.client.pipeline() as pipe:
            pipe.hset(self._node_key(name), mapping=payload)
            pipe.sadd(self._all_nodes_key(), name)
            pipe.sadd(self._type_nodes_key(node_type), name)
            pipe.execute()

    def add_edge(self, name1: str, name2: str, edge_type: str) -> None:
        if not self.client.exists(self._node_key(name1)):
            raise ValueError(f"Unknown node: {name1}")
        if not self.client.exists(self._node_key(name2)):
            raise ValueError(f"Unknown node: {name2}")

        with self.client.pipeline() as pipe:
            pipe.sadd(self._adjacent_key(name1), name2)
            pipe.sadd(self._edge_adjacent_key(name1, edge_type), name2)
            pipe.execute()

    def get_node(self, name: str) -> dict[str, Any] | None:
        data = self.client.hgetall(self._node_key(name))
        if not data:
            return None
        properties = json.loads(data.get("properties", "{}"))
        return {
            "name": data["name"],
            "type": data["type"],
            "properties": properties,
        }

    def get_adjacent(
        self,
        name: str,
        node_type: str | None = None,
        edge_type: str | None = None,
    ) -> list[str]:
        key = self._edge_adjacent_key(name, edge_type) if edge_type else self._adjacent_key(name)
        adjacent = sorted(self.client.smembers(key))

        if node_type is None:
            return adjacent

        filtered: list[str] = []
        for adjacent_name in adjacent:
            node = self.get_node(adjacent_name)
            if node and node["type"] == node_type:
                filtered.append(adjacent_name)
        return filtered

    def get_recommendations(self, name: str) -> list[str]:
        owned_books = set(self.get_adjacent(name, node_type="Book", edge_type="bought"))
        recommendations: set[str] = set()

        for person in self.get_adjacent(name, node_type="Person", edge_type="knows"):
            recommendations.update(self.get_adjacent(person, node_type="Book", edge_type="bought"))

        return sorted(recommendations - owned_books)


def create_client(host: str = "localhost", port: int = 6379, db: int = 0) -> redis.Redis:
    return redis.Redis(host=host, port=port, db=db, decode_responses=True)
