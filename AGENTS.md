# Agent Conventions

## Project
- Databricks medallion pipeline: Bronze → Silver → Gold → Analytics → Monitoring
- DAB (Databricks Asset Bundle) deployment via `databricks.yml`
- Targets: `dev`, `prod`

## Notebooks
- All notebooks are `.py` Databricks notebook source format
- Every notebook (except utilities) must call PipelineLogger:
  - `%run ../00_utilities/logger` after `%run ../00_utilities/config`
  - `logger = PipelineLogger(spark, "pipeline_name", run_id)`
  - Uses `logger.start_task()` / `logger.complete_task()` / `logger.fail_task()`

## CI/CD
- GitHub Actions workflow: `.github/workflows/ci-cd.yml`
- Secrets needed: `DATABRICKS_TOKEN`
- Deploys to dev on `develop` branch push, to prod on `main` branch push

## Git Conventions
- Commit messages: concise, focus on "why", prefixed by type (fix, feat, refactor, docs)
- Branches: `feature/*`, `fix/*`, `docs/*` originating from `develop`
- PRs merge into `develop`, `develop` merges into `main` for releases

## Commands
- Lint: `flake8 notebooks/ --max-line-length=120 --ignore=E402,W503`
- Validate bundle: `databricks bundle validate --target dev --strict`
- Deploy dev: `databricks bundle deploy --target dev --auto-approve`
- Deploy uat: `databricks bundle deploy --target uat --auto-approve`
- Deploy prod: `databricks bundle deploy --target prod --auto-approve`
- Run pipeline dev: `databricks bundle run full-pipeline-job --target dev --refresh-all`
- Run pipeline uat: `databricks bundle run full-pipeline-job --target uat --refresh-all`

## Environments
- **dev**: Developer sandbox, `credit_card_dev` catalog, mode=development, schedule PAUSED
- **uat**: Shared test, `credit_card_uat` catalog, mode=development, schedule UNPAUSED
- **prod**: Production, `credit_card_prod` catalog, mode=production, schedule UNPAUSED, full alerting
