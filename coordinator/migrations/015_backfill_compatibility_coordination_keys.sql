-- ============================================================================
-- CodeClaim: Migration 015 - Backfill and reconcile compatibility work
-- ============================================================================
-- Preserve every existing row, but retain one canonical row per semantic
-- obligation. Older duplicate rows become explicit historical SUPERSEDED rows.

UPDATE compatibility_work_items
SET coordination_key = concat(
        'compat:',
        source_contract_id::STRING,
        ':',
        source_contract_revision::STRING,
        ':',
        coalesce(payload->>'interface_dependency_id', concat('legacy:', work_item_id::STRING))
    )
WHERE coordination_key IS NULL;

WITH ranked AS (
    SELECT
        work_item_id,
        coordination_key,
        row_number() OVER (
            PARTITION BY coordination_key
            ORDER BY
                CASE WHEN state IN (
                    'PENDING', 'DISPATCHED', 'ACKNOWLEDGED', 'EXECUTING',
                    'AWAITING_APPROVAL', 'VERIFIED', 'REVIEW_REQUIRED',
                    'BLOCKED', 'INCOMPATIBLE'
                ) THEN 0 ELSE 1 END,
                created_at DESC,
                work_item_id DESC
        ) AS duplicate_rank
    FROM compatibility_work_items
    WHERE coordination_key IS NOT NULL
)
UPDATE compatibility_work_items AS work
SET state = 'SUPERSEDED',
    coordination_key = concat('superseded:', work.work_item_id::STRING),
    failure_reason = coalesce(work.failure_reason, 'Superseded by the canonical compatibility obligation')
FROM ranked
WHERE work.work_item_id = ranked.work_item_id
  AND ranked.duplicate_rank > 1;
