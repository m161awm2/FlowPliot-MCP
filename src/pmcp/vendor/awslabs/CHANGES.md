# Vendored awslabs/mcp code

Source: https://github.com/awslabs/mcp @ `24c39705d1d762cb6ebdd72b23f757a67b7a36c2` (2026-10-23), Apache-2.0 (see `LICENSE`, `NOTICE`).

| File | Upstream path | Changes |
|---|---|---|
| `sql_analyzer.py` | `src/aws-dataprocessing-mcp-server/.../utils/sql_analyzer.py` | Header note added only. |
| `sensitive_data_filter.py` | `src/aws-dataprocessing-mcp-server/.../utils/sensitive_data_filter.py` | Header note added only. pmcp adds its own allowlists in `pmcp/aws/redaction.py` and does not use the upstream `redaction_notice` text. |

Patterns referenced but re-implemented (not copied): the `ManagedBy` ownership tag check
(`AwsHelper.is_resource_mcp_managed`) → `pmcp/aws/ownership.py`; Redshift's `asyncio.to_thread`
+ long-poll executor → `pmcp/aws/client.py` (`run_sync`) and the worker poll loops.
