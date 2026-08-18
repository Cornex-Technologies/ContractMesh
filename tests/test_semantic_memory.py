"""Section 5 Verification Suite: LangChain × CockroachDB Semantic Memory & Checkpoint Engine."""

import json
import math
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from langchain_core.documents import Document

from coordinator.memory import (
    DeterministicEmbeddings,
    discover_and_verify_dependencies,
    get_cockroach_vectorstore,
    get_langgraph_checkpointer,
    load_agent_task,
    load_langgraph_checkpoint,
    save_agent_checkpoint,
    save_langgraph_checkpoint,
    search_candidate_contracts,
    store_contract_semantic_memory,
)
from coordinator.db import check_health, close_pool, execute_query, execute_statement, init_db


# ==============================================================================
# 1. Embedding Provider & Vector Mathematics Unit Tests
# ==============================================================================


def test_deterministic_embeddings_dimensions_and_normalization():
    """Verify that deterministic embeddings produce unit-length 1536-dimensional vectors."""
    provider = DeterministicEmbeddings(dimension=1536)
    
    vec1 = provider.embed_query("Process credit card payment and billing charge")
    vec2 = provider.embed_query("Charge customer order with payment token")
    vec3 = provider.embed_query("User authentication login session refresh")

    assert len(vec1) == 1536
    assert len(vec2) == 1536
    assert len(vec3) == 1536

    # Verify unit length L2 norm
    norm1 = math.sqrt(sum(x * x for x in vec1))
    norm2 = math.sqrt(sum(x * x for x in vec2))
    assert abs(norm1 - 1.0) < 1e-4
    assert abs(norm2 - 1.0) < 1e-4

    # Verify semantic cluster proximity
    sim_related = sum(a * b for a, b in zip(vec1, vec2))
    sim_unrelated = sum(a * b for a, b in zip(vec1, vec3))
    assert sim_related > sim_unrelated, f"Expected {sim_related} > {sim_unrelated}"


# ==============================================================================
# 2. LangChain Vector Store Operational Path Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_store_contract_semantic_memory_routes_via_langchain_vectorstore():
    """Verify store_contract_semantic_memory adds text and metadata directly via LangChain AsyncCockroachDBVectorStore."""
    mock_vstore = AsyncMock()
    mock_vstore.aadd_texts = AsyncMock(return_value=["mem-langchain-101"])

    with patch("coordinator.memory.get_cockroach_vectorstore", return_value=mock_vstore):
        memory_id = await store_contract_semantic_memory(
            contract_revision_id="rev-123",
            service_name="billing-service",
            endpoint_path="/v1/charges",
            http_method="POST",
            summary="Process credit card payments",
            metadata={"tier": "production"},
            use_langchain_store=True,
        )

        assert memory_id == "mem-langchain-101"
        mock_vstore.aadd_texts.assert_awaited_once()
        call_args = mock_vstore.aadd_texts.call_args[1]
        assert call_args["texts"] == ["Process credit card payments"]
        assert call_args["metadatas"][0]["contract_revision_id"] == "rev-123"
        assert call_args["metadatas"][0]["service_name"] == "billing-service"
        assert call_args["metadatas"][0]["tier"] == "production"


@pytest.mark.asyncio
async def test_search_candidate_contracts_routes_via_langchain_vectorstore():
    """Verify search_candidate_contracts queries directly through LangChain AsyncCockroachDBVectorStore."""
    doc = Document(
        page_content="Billing Service Charge API v1",
        metadata={
            "memory_type": "service_contract",
            "service_name": "billing-service",
            "endpoint_path": "/v1/charges",
            "http_method": "POST",
            "contract_revision_id": "rev-123",
        },
    )
    doc.id = "mem-101"

    mock_vstore = AsyncMock()
    # Distance of 0.12 -> Similarity score 0.88
    mock_vstore.asimilarity_search_with_score = AsyncMock(return_value=[(doc, 0.12)])

    with patch("coordinator.memory.get_cockroach_vectorstore", return_value=mock_vstore):
        candidates = await search_candidate_contracts("How to charge card for orders?", limit=3, use_langchain_store=True)

        assert len(candidates) == 1
        cand = candidates[0]
        assert cand["service_name"] == "billing-service"
        assert cand["endpoint_path"] == "/v1/charges"
        assert cand["http_method"] == "POST"
        assert cand["contract_revision_id"] == "rev-123"
        assert cand["similarity_score"] == 0.88
        assert cand["distance"] == 0.12
        mock_vstore.asimilarity_search_with_score.assert_awaited_once_with(query="How to charge card for orders?", k=3)


# ==============================================================================
# 3. LangGraph Checkpointer Operational Path Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_langgraph_checkpoint_save_and_load_routes_via_async_cockroach_saver():
    """Verify save_langgraph_checkpoint and load_langgraph_checkpoint route directly via AsyncCockroachDBSaver."""
    mock_saver = AsyncMock()
    mock_saver.aput = AsyncMock(return_value={"configurable": {"checkpoint_id": "chk-001"}})
    mock_saver.aget_tuple = AsyncMock(return_value=("config_tuple", "checkpoint_data", "meta", "parent"))

    class MockAsyncContextManager:
        async def __aenter__(self):
            return mock_saver
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    with patch("coordinator.memory.get_langgraph_checkpointer", return_value=MockAsyncContextManager()):
        config = {"configurable": {"thread_id": "task-orders-102", "checkpoint_ns": ""}}
        checkpoint_payload = {
            "v": 1,
            "ts": "2026-08-17T12:00:00Z",
            "channel_values": {"agent_state": {"step": "reconcile"}},
            "channel_versions": {"agent_state": 1},
            "versions_seen": {},
        }
        metadata = {"plan_revision": 2, "status": "REPLANNING"}

        # 1. Save checkpoint through LangGraph checkpointer
        save_res = await save_langgraph_checkpoint(config, checkpoint_payload, metadata)
        assert save_res["configurable"]["checkpoint_id"] == "chk-001"
        mock_saver.aput.assert_awaited_once_with(config, checkpoint_payload, metadata, {})

        # 2. Load checkpoint through LangGraph checkpointer
        loaded_tuple = await load_langgraph_checkpoint(config)
        assert loaded_tuple is not None
        assert loaded_tuple[1] == "checkpoint_data"
        mock_saver.aget_tuple.assert_awaited_once_with(config)


# ==============================================================================
# 4. Tri-Layer Discovery Pipeline & Relational Gate Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_tri_layer_discovery_pipeline_mocked():
    """Verify Tri-Layer Discovery: Vector Candidate Search -> Relational Verification -> Active Contract."""
    mock_candidates = [
        {
            "memory_id": "mem-1",
            "service_name": "billing-service",
            "endpoint_path": "/v1/charges",
            "http_method": "POST",
            "contract_revision_id": "rev-1",
            "similarity_score": 0.92,
            "distance": 0.08,
        }
    ]

    mock_relational_row = {
        "contract_id": "ctr-101",
        "provider_service": "billing-service",
        "endpoint_path": "/v1/charges",
        "http_method": "POST",
        "contract_revision_id": "rev-1",
        "revision_number": 1,
        "schema_json": {"properties": {"card_token": {"type": "string"}}},
        "semantic_summary": "Charge payment v1",
        "source_commit": "commit-billing-v1",
        "dependency_id": "77777777-7777-7777-7777-777777777777",
    }

    with patch("coordinator.memory.search_candidate_contracts", AsyncMock(return_value=mock_candidates)), \
         patch("coordinator.memory.fetch_one", AsyncMock(return_value=mock_relational_row)):
        
        verified = await discover_and_verify_dependencies(
            consumer_service="orders-service",
            task_prompt="Implement 1-click checkout by charging billing service",
            limit=5,
            verified_only=False,
        )

        assert len(verified) == 1
        dep = verified[0]
        assert dep["provider_service"] == "billing-service"
        assert dep["endpoint_path"] == "/v1/charges"
        assert dep["active_revision_number"] == 1
        assert dep["is_verified_consumer"] is True
        assert "card_token" in dep["schema_json"]["properties"]


@pytest.mark.asyncio
async def test_tri_layer_discovery_relational_confirmation_gate():
    """Verify that verified_only=True enforces the relational confirmation gate, rejecting unverified candidates."""
    mock_candidates = [
        {
            "memory_id": "mem-unverified",
            "service_name": "untrusted-service",
            "endpoint_path": "/v1/data",
            "http_method": "GET",
            "contract_revision_id": "rev-99",
            "similarity_score": 0.89,
            "distance": 0.11,
        }
    ]

    mock_unverified_row = {
        "contract_id": "ctr-999",
        "provider_service": "untrusted-service",
        "endpoint_path": "/v1/data",
        "http_method": "GET",
        "contract_revision_id": "rev-99",
        "revision_number": 1,
        "schema_json": {"properties": {}},
        "semantic_summary": "Untrusted API",
        "source_commit": "commit-untrusted",
        "dependency_id": None,
    }

    with patch("coordinator.memory.search_candidate_contracts", AsyncMock(return_value=mock_candidates)), \
         patch("coordinator.memory.fetch_one", AsyncMock(return_value=mock_unverified_row)):
        
        results = await discover_and_verify_dependencies(
            consumer_service="orders-service",
            task_prompt="Query arbitrary untrusted data",
            verified_only=True,
        )
        assert len(results) == 0, "Unverified candidate must be rejected by the relational confirmation gate"


# ==============================================================================
# 5. Agent Checkpoint State Persistence Tests
# ==============================================================================


@pytest.mark.asyncio
async def test_agent_checkpoint_save_persists_full_checkpoint_state():
    """Verify that save_agent_checkpoint persists checkpoint_state payload into both task table and history."""
    executed_queries = []

    mock_conn = MagicMock()
    async def mock_execute(query, params=None):
        executed_queries.append((query, params))

    mock_conn.execute = mock_execute

    with patch("coordinator.memory.run_transaction") as mock_run_tx:
        async def run_tx_side_effect(fn):
            return await fn(mock_conn)
        mock_run_tx.side_effect = run_tx_side_effect

        checkpoint_data = {
            "node": "replan_step",
            "scratchpad": "Handling breaking payment_method_id schema migration",
            "adapted_files": ["clients/billing_client.py"],
            "pending_subtasks": ["run_contract_tests"],
        }

        await save_agent_checkpoint(
            task_id="task-orders-102",
            plan_revision=3,
            status="REPLANNING",
            checkpoint_state=checkpoint_data,
        )

        assert len(executed_queries) == 2
        sql1, params1 = executed_queries[0]
        assert "active_agent_tasks" in sql1
        assert "checkpoint_state" in sql1
        assert params1[0] == 3  # plan_revision
        assert params1[1] == "REPLANNING"  # status
        assert json.loads(params1[2]) == checkpoint_data  # checkpoint_state JSON
        assert params1[3] == "task-orders-102"  # task_id

        sql2, params2 = executed_queries[1]
        assert "agent_checkpoints" in sql2
        assert params2[0] == "task-orders-102"
        assert params2[1] == 3
        assert params2[2] == "REPLANNING"
        assert json.loads(params2[3]) == checkpoint_data


@pytest.mark.asyncio
async def test_agent_checkpoint_load_retrieves_checkpoint_state():
    """Verify load_agent_task returns persisted checkpoint_state."""
    mock_task_row = {
        "task_id": "task-orders-102",
        "agent_id": "agent-b",
        "service_name": "orders-service",
        "task_prompt": "Build checkout flow",
        "worktree_path": "worktrees/task-orders-102",
        "base_commit": "commit-orders-base",
        "plan_revision": 2,
        "status": "REPLANNING",
        "checkpoint_state": {
            "node": "reconcile",
            "scratchpad": "Active drift detected",
        },
        "created_at": "2026-08-17T12:00:00Z",
        "updated_at": "2026-08-17T12:05:00Z",
    }
    mock_deps_rows = [
        {
            "provider_service": "billing-service",
            "contract_id": "ctr-101",
            "assumed_revision": 1,
            "dependency_kind": "HTTP_REST",
            "dependency_path": "clients/billing_client.py",
        }
    ]

    with patch("coordinator.memory.fetch_one", AsyncMock(return_value=mock_task_row)), \
         patch("coordinator.memory.execute_query", AsyncMock(return_value=mock_deps_rows)):
        
        task = await load_agent_task("task-orders-102")
        assert task is not None
        assert task["agent_id"] == "agent-b"
        assert task["plan_revision"] == 2
        assert task["status"] == "REPLANNING"
        assert task["checkpoint_state"]["node"] == "reconcile"
        assert len(task["dependencies"]) == 1
        assert task["dependencies"][0]["provider_service"] == "billing-service"
        assert task["dependencies"][0]["assumed_revision"] == 1


# ==============================================================================
# 6. Live CockroachDB Integration Test
# ==============================================================================


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_semantic_memory_vector_indexing():
    """Live test: Inserts semantic memory embedding into CockroachDB and executes vector search."""
    health = await check_health(timeout_seconds=3.0)
    if health.get("status") != "healthy":
        pytest.skip(f"Live CockroachDB is not reachable: {health.get('error')}")

    try:
        await init_db()

        # Store semantic contract memory
        memory_id = await store_contract_semantic_memory(
            contract_revision_id="22222222-2222-2222-2222-222222222222",
            service_name="billing-service",
            endpoint_path="/v1/charges",
            http_method="POST",
            summary="Process credit card payments, charges, and refunds",
            metadata={"version": 1},
        )
        assert memory_id is not None

        # Search candidate contracts using semantic query
        candidates = await search_candidate_contracts("How to charge customers for purchases?", limit=3)
        assert len(candidates) >= 1
        top = candidates[0]
        assert top["service_name"] == "billing-service"
        assert top["endpoint_path"] == "/v1/charges"

    finally:
        await close_pool()
