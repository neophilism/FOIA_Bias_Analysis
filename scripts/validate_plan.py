"""Validate this repository's canonical completion plan, dependencies and PR evidence.

Standard-library only. This checks documentation/registry integrity, not product tests.
"""
import json
import re
from datetime import datetime
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def utc(value):
    require(isinstance(value, str) and value.endswith('Z'), 'Expected UTC timestamp')
    datetime.fromisoformat(value.replace('Z', '+00:00'))


def main():
    root = Path(__file__).resolve().parents[1]
    doc = json.loads((root / 'registry/prs.json').read_text())
    plan = (root / 'docs/DEVELOPMENT_PLAN.md').read_text()
    require((root / 'docs/HANDOFF_STATUS.md').is_file(), 'Missing handoff entry point')
    require(doc['schemaVersion'] == 1, 'Unsupported registry schema')
    require(doc['repository'] in ('neophilism/FOIA_Bias_Analysis', 'neophilism/Send_FOIA_Log_Requests'), 'Wrong repository identity')
    prefix = 'FBA' if doc['repository'].endswith('/FOIA_Bias_Analysis') else 'SFL'
    requirement_prefix = 'BR' if prefix == 'FBA' else 'SR'
    require(type(doc['roadmapVersion']) is int and doc['roadmapVersion'] > 0, 'Invalid roadmap version')
    utc(doc['observedAt'])
    units = doc['units']
    require(type(doc['plannedPrCount']) is int and doc['plannedPrCount'] == len(units) > 0, 'Plan denominator mismatch')
    require(doc['planVerified'] is True, 'Current enumeration must be explicit')
    provenance = doc['planProvenance']
    require(type(provenance['historicalPlanVerified']) is bool and type(provenance['handoffCertified']) is bool, 'Certification dimensions must be explicit')
    headings = re.findall(r'^### (' + prefix + r'-\d{2}) — (.+)$', plan, re.M)
    require(headings == [(u['id'], u['title']) for u in units], 'Plan headings and ordered registry units differ')
    ids = {u['id'] for u in units}
    require(len(ids) == len(units), 'Duplicate work ID')
    numbers = set()
    for p in doc['pullRequests']:
        require(type(p['number']) is int and p['number'] > 0 and p['number'] not in numbers, 'Invalid/duplicate PR number')
        numbers.add(p['number'])
        require(type(p['id']) is int and p['id'] > 0 and p['title'].strip(), 'Invalid actual PR identity')
        require(p['url'] == 'https://github.com/' + doc['repository'] + '/pull/' + str(p['number']), 'Wrong PR repository/URL')
        require(p['state'] in ('open', 'closed') and type(p['draft']) is bool, 'Invalid PR state')
        for field in ('createdAt', 'updatedAt'):
            utc(p[field])
        for field in ('closedAt', 'mergedAt'):
            if p[field] is not None:
                utc(p[field])
        require(not (p['state'] == 'open' and (p['closedAt'] or p['mergedAt'])), 'Open PR has close/merge time')
        require(not p['mergedAt'] or p['state'] == 'closed', 'Merged PR is not closed')
        require(not p['mergedAt'] or datetime.fromisoformat(p['mergedAt'].replace('Z', '+00:00')) <= datetime.fromisoformat(doc['observedAt'].replace('Z', '+00:00')), 'Future merge evidence')
    graph = {}
    for u in units:
        require(u['origin'] in ('original', 'addition'), 'Invalid scope origin')
        require(u['scopeBasis'].strip() and u['requirementIds'], 'Missing scope provenance')
        require(u['planPath'] == 'docs/DEVELOPMENT_PLAN.md' and u['planSection'] == u['id'], 'Invalid plan reference')
        for req in u['requirementIds']:
            require(re.fullmatch(requirement_prefix + r'-\d{2}', req) is not None and '| ' + req + ' |' in plan, 'Unknown requirement: ' + req)
        for key in ('prs', 'priorEvidencePrs'):
            require(len(set(u[key])) == len(u[key]) and all(type(n) is int and n in numbers for n in u[key]), 'Missing/duplicate actual PR evidence for ' + u['id'])
        require(len(set(u['dependsOn'])) == len(u['dependsOn']) and all(d in ids and d != u['id'] for d in u['dependsOn']), 'Invalid prerequisite for ' + u['id'])
        require(all(isinstance(d, str) and d.strip() for d in u['externalPrerequisites']), 'Invalid external prerequisite')
        graph[u['id']] = u['dependsOn']
    active, done = set(), set()
    def visit(ident):
        require(ident not in active, 'Cyclic prerequisites at ' + ident)
        if ident in done:
            return
        active.add(ident)
        for dep in graph[ident]:
            visit(dep)
        active.remove(ident)
        done.add(ident)
    for ident in ids:
        visit(ident)
    redirect = json.loads((root / 'registry/roadmap.json').read_text())
    require(redirect.get('redirectTo') == 'registry/prs.json', 'Legacy roadmap must redirect to canonical registry')
    require('milestones' not in redirect and 'plannedPrCount' not in redirect, 'Duplicate legacy plan denominator')
    for path in (root / 'README.md', root / 'docs/DEVELOPMENT_PLAN.md', root / 'docs/HANDOFF_STATUS.md'):
        text = path.read_text()
        for target in re.findall(r'\]\(([^)]+)\)', text):
            if '://' in target or target.startswith('#'):
                continue
            require((path.parent / target.split('#')[0]).exists(), 'Missing local link: ' + str(path) + ' -> ' + target)
    print(f"PASS {doc['repository']}: {len(units)} units, {len(numbers)} PR records, plan links and prerequisite DAG valid; runtime/certification not tested")


if __name__ == '__main__':
    main()
