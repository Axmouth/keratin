#!/usr/bin/env python3
"""Convert a single-process Keratin batch capture to Chrome/Perfetto JSON.

No payloads or external dependencies. Monotonic clocks from different processes
are deliberately not aligned here.
"""
import argparse
import json
import sys
from pathlib import Path


def convert(rows):
    headers = [r for r in rows if r.get('type') == 'process']
    if len(headers) != 1 or headers[0].get('version') != 1:
        raise ValueError('expected one version-1 process capture')
    pid = headers[0]['pid']
    output = []
    writers = {r['writer']: r for r in rows if r.get('type') == 'writer'}
    summaries = [r for r in rows if r.get('type') == 'summary']
    complete = bool(summaries and summaries[-1].get('closed') and summaries[-1]['dropped'] == 0)
    if not complete:
        print('WARNING: incomplete capture (missing close marker or dropped events)', file=sys.stderr)
    physical_groups = {(r['writer'], r['id']) for r in rows if r.get('stage') == 'fsync_begin'}
    def lane(r):
        stage = r['stage']
        role = 'fsync' if (r['writer'], r['id']) in physical_groups and stage in ('fsync', 'fsync_begin', 'fsync_success', 'fsync_error', 'log_fsync', 'index_fsync') or stage == 'fsync_member' else ('notifier' if stage == 'notify' else 'writer')
        return r['writer'] * 3 + {'writer': 0, 'fsync': 1, 'notifier': 2}[role]
    for ident, writer in writers.items():
        for role in ('writer', 'fsync', 'notifier'):
            output.append(dict(ph='M', name='thread_name', pid=pid, tid=ident * 3 + {'writer': 0, 'fsync': 1, 'notifier': 2}[role],
                               args={'name': f"{writer['path']} [{role}]"}))
    events = [r for r in rows if 'stage' in r]
    queued = {}
    groups = {}
    for r in events:
        args = {k: v for k, v in r.items() if k not in ('start_ns', 'end_ns', 'stage') and v is not None}
        start, end = r['start_ns'] / 1000, r['end_ns'] / 1000
        event = dict(name=r['stage'], cat='keratin.batch', pid=pid, tid=lane(r), ts=start, args=args)
        if end > start:
            event.update(ph='X', dur=end-start)
        else:
            event.update(ph='i', s='t')
        output.append(event)
        if r['stage'] in ('commit_queued', 'sync_queued'):
            queued[(r['writer'], r['id'])] = r
        elif r['stage'] == 'fsync_begin':
            groups[(r['writer'], r['id'])] = r
    # Explicit edges only. A shared offset bound alone is not enough evidence to
    # connect operations across epoch changes, suffix repair or checkpoints.
    for r in events:
        if r['stage'] != 'fsync_member':
            continue
        source = queued.get((r['writer'], r['id']))
        target = groups.get((r['writer'], r['parent']))
        if source and target:
            ident = f"{r['writer']}:{r['id']}:{r['parent']}"
            output.append(dict(ph='s', name='commit to fsync', cat='keratin.batch', pid=pid,
                               tid=lane(source), ts=source['end_ns']/1000, id=ident))
            output.append(dict(ph='f', bp='e', name='commit to fsync', cat='keratin.batch', pid=pid,
                               tid=lane(target), ts=target['end_ns']/1000, id=ident))
    return {'traceEvents': output, 'displayTimeUnit': 'ms',
            'metadata': {'complete': complete, 'process': headers[0], 'summaries': summaries}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    with args.input.open() as source:
        rows = [json.loads(line) for line in source if line.strip()]
    result = convert(rows)
    with args.output.open('x') as output:
        json.dump(result, output)
    print(f"Wrote {len(result['traceEvents'])} events to {args.output}")


if __name__ == '__main__':
    main()
