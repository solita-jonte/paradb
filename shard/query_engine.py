"""Query engine — matches documents against MongoDB-like query filters."""

from typing import Any

from .document_storage import scan_all_documents


def _resolve_dotted_field(document: dict[str, Any], field: str) -> tuple[bool, Any]:
    """Resolve a dot-notation field path. Returns (found, value)."""
    parts = field.split(".")
    current = document
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _match_value(doc_value: Any, query_value: Any) -> bool:
    """Match a single field value against a query value (which may contain operators)."""
    if isinstance(query_value, dict):
        for op, op_val in query_value.items():
            if op == "$gt":
                if not (doc_value > op_val):
                    return False
            elif op == "$lt":
                if not (doc_value < op_val):
                    return False
            elif op == "$exists":
                return False  # handled at caller level
            else:
                return False
        return True
    return doc_value == query_value


def match(document: dict[str, Any], query: dict[str, Any]) -> bool:
    """Return True if document matches the query filter."""
    for field, query_value in query.items():
        found, doc_value = _resolve_dotted_field(document, field)

        if isinstance(query_value, dict) and "$exists" in query_value:
            if query_value["$exists"] and not found:
                return False
            if not query_value["$exists"] and found:
                return False
            continue

        if not found:
            return False

        if not _match_value(doc_value, query_value):
            return False

    return True


def execute_query(data_dir: str, query: dict[str, Any]) -> list[dict[str, Any]]:
    """Scan all documents on disk and return those matching the query."""
    docs = scan_all_documents(data_dir)
    return [doc for doc in docs if match(doc, query)]
