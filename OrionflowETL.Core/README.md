# OrionflowETL.Core

This package provides the **core execution engine** for OrionflowETL.

It defines:
- Schema and row abstractions
- Pipeline execution contracts
- Deterministic, schema-first execution

## What this package does
- Executes pipelines defined by contracts
- Has no IO or infrastructure dependencies
- Is suitable as a runtime for higher-level ETL tools

## What this package does NOT do
- Read CSV files
- Connect to databases
- Infer schemas
- Perform automatic type conversion

Those features are provided by separate packages.

## Status
Early development (0.x).
The API is stable but may evolve.
