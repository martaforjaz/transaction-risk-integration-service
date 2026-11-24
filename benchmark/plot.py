"""Render the observed outage and recovery from benchmark/results.json."""
import json
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

folder = Path(__file__).parent
trial = json.loads((folder / 'results.json').read_text())['worker_outage']
samples = trial['timeline']
times = [sample['s'] for sample in samples]
plt.rcParams.update({'axes.spines.top': False, 'axes.spines.right': False, 'font.size': 10})
fig, ax = plt.subplots(figsize=(8.4, 3.3), layout='constrained')
ax.axvspan(trial['worker_stopped_s'], trial['worker_start_requested_s'],
           color='#e6c8d6', alpha=.45, label='Consumer stopped')
ax.axvline(trial['worker_start_requested_s'], color='#6b5163', linestyle='--', linewidth=1)
for key, label, color in [('pending','Unfinished','#986078'), ('decided','Decided','#275d82')]:
    ax.step(times, [sample[key] for sample in samples], where='post',
            label=label, color=color, linewidth=2)
ax.set(xlabel='Elapsed time (seconds)', ylabel='Transactions', ylim=(0, trial['transactions']*1.08))
ax.legend(frameon=False, loc='upper left')
fig.savefig(folder / 'worker-recovery.png', dpi=220)
