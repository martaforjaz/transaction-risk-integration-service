"""Run a local load, outage, recovery and duplicate submission benchmark."""
import json
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from statistics import median, quantiles
from time import monotonic, sleep
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ['docker', 'compose', '-p', 'risk-benchmark', '-f', 'compose.yaml', '-f', 'benchmark/compose.yaml']
BASE = 'http://127.0.0.1:18080/transactions/gateway'
ORIGIN = monotonic()


def now():
    return monotonic() - ORIGIN


def compose(*args, stdin=None):
    return subprocess.run([*COMPOSE, *args], cwd=ROOT, input=stdin, text=True,
                          stdout=subprocess.PIPE, check=True).stdout


def submit(identifier):
    payload = json.dumps({'id': identifier, 'amountMinor': 1250, 'currency': 'EUR'}).encode()
    request = Request(BASE, payload, {'Content-Type': 'application/json'})
    with urlopen(request, timeout=5) as response:
        result = json.load(response)
    assert result['id'] == identifier
    return now()


def measure(rate, count, outage=False):
    ids = [str(uuid4()) for _ in range(count)]
    ids_sql = ','.join(f"'{item}'" for item in ids)
    query = f"SELECT id,status,attempts,decision FROM transactions WHERE id IN ({ids_sql});"
    observations, series = {}, []
    stop = threading.Event()
    failure = []
    restarts = {}

    def rows():
        return [line.split('|') for line in compose('exec', '-T', 'db', 'psql', '-U', 'risk',
                '-d', 'risk', '-At', '-v', 'ON_ERROR_STOP=1', '-c', query).splitlines()]

    def monitor():
        try:
            while not stop.is_set():
                states = rows()
                timestamp = now()
                decided = [row[0] for row in states if row[1] == 'DECIDED']
                for identifier in decided:
                    observations.setdefault(identifier, timestamp)
                series.append({'s': round(timestamp, 3), 'received': len(states),
                               'decided': len(decided), 'pending': len(states)-len(decided)})
                stop.wait(.35)
        except Exception as exc:
            failure.append(exc)

    watcher = threading.Thread(target=monitor)
    watcher.start()
    started = now()
    try:
        with ThreadPoolExecutor(max_workers=12) as pool:
            if outage:
                compose('stop', '-t', '2', 'worker')
                restarts['worker_stopped_s'] = now()
            first_send = now()
            futures = []
            for index, identifier in enumerate(ids):
                sleep(max(0, first_send + index/rate - now()))
                futures.append(pool.submit(submit, identifier))
            accepted = dict(zip(ids, (future.result() for future in futures)))
            if outage:
                sleep(1)
                if not series or series[-1]['pending'] != count:
                    raise RuntimeError('Expected every request to wait while the consumer was stopped.')
                restarts['worker_start_requested_s'] = now()
                compose('start', 'worker')
            deadline = now() + 100
            while len(observations) < count and not failure and now() < deadline:
                sleep(.2)
            if failure:
                raise failure[0]
            if len(observations) != count:
                raise RuntimeError(f'Only {len(observations)} of {count} decisions were observed.')
            duplicate_ids = ids[::5]
            list(pool.map(submit, duplicate_ids))
            final = rows()
            if len(final) != count or any(row[1:] != ['DECIDED','1','ALLOW'] for row in final):
                raise RuntimeError('Database reconciliation failed.')
    finally:
        stop.set()
        watcher.join()
        if outage and 'worker_start_requested_s' not in restarts:
            compose('start', 'worker')
    last = max(observations.values())
    latencies = [max(0, observations[key] - accepted[key]) for key in ids]
    recovery = last - restarts['worker_start_requested_s'] if outage else None
    return {'target_rps': rate, 'transactions': count, 'duplicate_submissions': len(duplicate_ids),
            'decisions': len(final), 'attempts_per_transaction': 1,
            'peak_unfinished': max(row['pending'] for row in series),
            'observed_p50_seconds': round(median(latencies), 3),
            'observed_p95_seconds': round(quantiles(latencies,n=100)[94], 3),
            'end_to_end_decisions_per_second': round(count / (last - first_send), 2),
            'recovery_seconds': round(recovery, 3) if outage else None,
            'timeline': [dict(row, s=round(row['s'] - started, 3)) for row in series],
            **{k: round(v - started, 3) for k,v in restarts.items()}}


if __name__ == '__main__':
    measure(5, 5)  # Warm the separate HTTP API and Kafka consumer.
    result = {'baseline_10': measure(10, 60), 'baseline_30': measure(30, 120),
              'worker_outage': measure(25, 125, outage=True)}
    (ROOT / 'benchmark/results.json').write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    for name, scenario in result.items():
        print(name, {k:v for k,v in scenario.items() if k != 'timeline'})
