"""CodeClaim Configuration & Environment Validation."""

from __future__ import annotations

from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings for CodeClaim Coordinator & Contract Mesh."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # CockroachDB Configuration
    cockroach_database_url: Optional[str] = Field(
        default=None,
        validation_alias="COCKROACH_DATABASE_URL",
        description="PostgreSQL/CockroachDB connection string",
    )

    # AWS Configuration
    aws_region: str = Field(
        default="us-east-1",
        validation_alias="AWS_REGION",
    )
    aws_access_key_id: Optional[str] = Field(
        default=None,
        validation_alias="AWS_ACCESS_KEY_ID",
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        validation_alias="AWS_SECRET_ACCESS_KEY",
    )
    bedrock_api_key: Optional[str] = Field(
        default=None,
        validation_alias="BEDROCK_API_KEY",
        description="Amazon Bedrock Bearer API Key",
    )

    # Amazon Bedrock Models
    bedrock_model_id: str = Field(
        default="anthropic.claude-3-5-sonnet-20241022-v2:0",
        validation_alias="BEDROCK_MODEL_ID",
        description="Bedrock LLM model for agent reasoning and code adaptation",
    )
    bedrock_embedding_model_id: str = Field(
        default="amazon.titan-embed-text-v1",
        validation_alias="BEDROCK_EMBEDDING_MODEL_ID",
        description="Bedrock embedding model for contract summaries",
    )
    bedrock_embedding_provider: str = Field(
        default="titan",
        validation_alias="BEDROCK_EMBEDDING_PROVIDER",
        description="Embedding provider payload protocol: titan or cohere_v4",
    )
    embedding_dimension: int = Field(
        default=1536,
        validation_alias="EMBEDDING_DIMENSION",
        description="Vector dimension; must match semantic_memory VECTOR dimension.",
    )

    # Amazon S3 Receipt Archive
    s3_receipt_bucket: str = Field(
        default="codeclaim-audit-receipts",
        validation_alias="S3_RECEIPT_BUCKET",
    )

    # Coordination & Security Settings (Safe Defaults)
    demo_auto_reconcile: bool = Field(
        default=False,
        validation_alias="DEMO_AUTO_RECONCILE",
        description="Safe default False. Set True only for scripted 3-minute video demo flows.",
    )
    is_demo_mode: bool = Field(
        default=False,
        validation_alias="IS_DEMO_MODE",
        description="When True, relaxes strict secret requirements for local offline testing.",
    )
    demo_allow_anonymous_mutations: bool = Field(
        default=False,
        validation_alias="DEMO_ALLOW_ANONYMOUS_MUTATIONS",
        description="When False (default), demo mode requires a demo access token for mutating endpoints.",
    )
    public_demo_enabled: bool = Field(
        default=False,
        validation_alias="PUBLIC_DEMO_ENABLED",
        description="Expose the bounded no-auth public demo workflow. Keep disabled for private deployments.",
    )
    public_demo_run_timeout_seconds: int = Field(
        default=900,
        validation_alias="PUBLIC_DEMO_RUN_TIMEOUT_SECONDS",
        ge=60,
        le=3600,
        description="Maximum age before a public demo RUNNING record may be recovered.",
    )

    coordinator_host: str = Field(
        default="0.0.0.0",
        validation_alias="COORDINATOR_HOST",
    )
    coordinator_port: int = Field(
        default=8000,
        validation_alias="COORDINATOR_PORT",
    )
    mcp_cluster_id: Optional[str] = Field(
        default=None,
        validation_alias="MCP_CLUSTER_ID",
        description="CockroachDB Cloud Cluster ID for Managed MCP connection",
    )

    # Coordinator API Key for Operator & Deployment Actions
    coordinator_api_key: Optional[str] = Field(
        default=None,
        validation_alias="COORDINATOR_API_KEY",
        description="Operator secret key for administrative and deployment actions",
    )

    # Webhook Shared Secret (for Changefeed Ingestion Authentication)
    changefeed_webhook_secret: Optional[str] = Field(
        default=None,

        validation_alias="CHANGEFEED_WEBHOOK_SECRET",
        description="Secret key required to authenticate incoming changefeed webhooks.",
    )
    harness_dispatch_webhook_secret: Optional[str] = Field(
        default=None,
        validation_alias="HARNESS_DISPATCH_WEBHOOK_SECRET",
        description="Shared webhook secret for registered harness runners. Use a secret manager in production.",
    )
    mcp_harness_id: Optional[str] = Field(default=None, validation_alias="MCP_HARNESS_ID")
    mcp_harness_token: Optional[str] = Field(default=None, validation_alias="MCP_HARNESS_TOKEN")

    # Slack is an optional, asynchronous observability projection of the outbox.
    slack_webhook_url: Optional[str] = Field(default=None, validation_alias="SLACK_WEBHOOK_URL")
    slack_notifications_enabled: bool = Field(default=False, validation_alias="SLACK_NOTIFICATIONS_ENABLED")
    slack_notify_checkpoints: bool = Field(default=False, validation_alias="SLACK_NOTIFY_CHECKPOINTS")

    @field_validator("coordinator_port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError(f"coordinator_port must be between 1 and 65535, got {v}")
        return v

    def validate_runtime(self, require_database: bool = True) -> None:
        """Validate critical configuration before server startup, aggregating all missing settings."""
        errors: list[str] = []

        if require_database and not self.cockroach_database_url:
            errors.append(
                "Missing 'COCKROACH_DATABASE_URL': A valid CockroachDB connection string is required."
            )
        if not self.is_demo_mode and not self.changefeed_webhook_secret:
            errors.append(
                "Missing 'CHANGEFEED_WEBHOOK_SECRET': An explicit shared secret is required outside demo mode."
            )
        if not self.is_demo_mode and not self.coordinator_api_key:
            errors.append(
                "Missing 'COORDINATOR_API_KEY': An explicit operator secret key is required outside demo mode."
            )

        if errors:
            formatted_errors = "\n  - " + "\n  - ".join(errors)
            raise ValueError(f"Runtime configuration validation failed:{formatted_errors}")


settings = Settings()
