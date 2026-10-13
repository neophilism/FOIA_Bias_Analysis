# Repository instructions

## Shared PR registry

Use `registry/prs.json` as the sole active PR plan for Boardroom and Engine Room. Follow `registry/PR_FORMAT.md` and run `node scripts/validate-pr-registry.mjs`. Preserve stable IDs, scope, counts, dependencies, acceptance and contribution-only evidence. `registry/roadmap.json` is retired; never recreate a duplicate milestone plan. This contract supersedes older file-path guidance.
