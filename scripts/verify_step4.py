import asyncio
import selectors
from coordinator.config import settings
from coordinator.memory import get_embeddings_provider, search_candidate_contracts, store_contract_semantic_memory
from coordinator.db import close_pool

async def verify_step4():
    print("--------------------------------------------------")
    print("CodeClaim Step 4: Bedrock Embeddings & Vector Search")
    print("--------------------------------------------------")
    print(f"AWS Region:             {settings.aws_region}")
    print(f"Bedrock Embedding Model:{settings.bedrock_embedding_model_id}")
    print(f"Embedding Provider:     {settings.bedrock_embedding_provider}")
    print(f"Configured Dimension:   {settings.embedding_dimension}")
    print("--------------------------------------------------")
    
    # 1. Test embedding generation
    embedder = get_embeddings_provider()
    sample_text = "Process customer credit card charge with tokenized payment method"
    print(f"Testing vector embedding for sample text:\n  '{sample_text}'")
    
    vector = embedder.embed_query(sample_text)
    print(f"  [✓] Embedding generated successfully!")
    print(f"  [✓] Vector Dimension: {len(vector)} (Expected: 1536)")
    print(f"  [✓] Vector Sample:     [{vector[0]:.4f}, {vector[1]:.4f}, {vector[2]:.4f}, ...]")
    
    # 2. Test CockroachDB Native Vector Search
    search_query = "charge credit card payment"
    print(f"\nTesting CockroachDB Native Vector Similarity Search for:\n  '{search_query}'")
    
    results = await search_candidate_contracts(search_query, limit=3, use_langchain_store=False)
    print(f"  [✓] Found {len(results)} matching candidate contract(s) via vector search:")
    for idx, r in enumerate(results, 1):
        score = r.get("similarity_score") or r.get("score") or 0.0
        print(f"    {idx}. {r.get('service_name')} {r.get('http_method')} {r.get('endpoint_path')} (v{r.get('revision_number')}) - Similarity: {score:.3f}")
        print(f"       Summary: {r.get('semantic_summary', '')[:80]}...")
    
    print("--------------------------------------------------")
    await close_pool()

if __name__ == "__main__":
    loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
    try:
        loop.run_until_complete(verify_step4())
    finally:
        loop.close()
