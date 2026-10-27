"""Owner-run two-configuration TorchFT/head-loss experiment."""
import hashlib
import json
import time

from evidence import write_json
from launcher import PYTHON, ROOT
from native_ml_pilot import NativeMLPilot


class NativeTorchFTPilot(NativeMLPilot):
    driver_script = 'native_torchft_workload.py'
    max_observation_timeout_s = 600

    def prepare(self):
        end = time.monotonic()+1680
        super().prepare()
        self.deadline = end
        try:
            versions = {}
            for vm in self.names:
                # Explicit dependency of native_probe.counters; do not upgrade
                # Ray, Torch or TorchFT in the experiment image.
                self.ssh(vm, f'sudo {PYTHON} -m pip install --disable-pip-version-check psutil==7.0.0', timeout=120)
                versions[vm] = json.loads(self.ssh(vm, f'{PYTHON} {ROOT}/{self.driver_script} preflight', timeout=45).stdout)
            if len({json.dumps(v, sort_keys=True) for v in versions.values()}) != 1:
                raise RuntimeError('Python/TorchFT dependency versions differ across VMs')
            write_json(self.local/'torchft-versions.json', versions)
            # Check actual counter collection before any experiment observation.
            counters = self.counters(self.names)
            if any('error' in c for c in counters.values()):
                raise RuntimeError('Resource counter preflight failed: '+str(counters))
        except BaseException:
            self.ready = False
            raise
        finally:
            self.deadline = None

    def workload_parameters(self):
        return dict(ml=self.ml_config, torchft=self.ft_config, pre_failure_reads=0)

    def report_metadata(self):
        return dict(ml_preparation=self.ml_preparation, torchft_versions=self.versions,
            interpretation='Two configurations only: real CPU TorchFT DDP with Fixed-R OFF/ON. '
                'Healthy and head-loss scenarios; driver, two training actors and Lighthouse survive. '
                'Fixed-R protects head-owned normal input tasks, not actors or optimizer state. '
                'No forced training pause, worker failure, application resubmission or checkpoint restart.')

    def run_torchft(self, name, settings, torchft_settings, *, repeats=1, budget_s=1680,
                    timeout_s=600, keepalive_interval_ms=10000, keepalive_timeout_ms=10000):
        self._check_name(name)
        if type(repeats) is not int or not 1 <= repeats <= 2:
            raise ValueError('Use one or two repetitions; resume uses the same settings')
        if type(timeout_s) is not int or not 300 <= timeout_s <= 600:
            raise ValueError('Observation timeout must be 300..600 seconds')
        saved = json.loads((self.local/('ml-preparation-'+name+'.json')).read_text())
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.bundle.glob('native_*.py')}
        if saved['settings'] != settings or saved['nodes'] != self.nodes or saved['helpers'] != hashes:
            raise ValueError('Settings/VMs/helpers changed; prepare a new suite')
        ft = dict(torchft_settings)
        if (type(ft['shards_per_rank']) is not int or not 2 <= ft['shards_per_rank'] <= 8 or
                type(ft['steps_per_shard']) is not int or not 1 <= ft['steps_per_shard'] <= 50 or
                type(ft['progress_every']) is not int or not 1 <= ft['progress_every'] <= 20 or
                not .05 <= ft['fault_fraction'] <= .3 or
                type(ft['collective_timeout_s']) is not int or not 120 <= ft['collective_timeout_s'] <= 240 or
                type(ft['input_timeout_s']) is not int or not 120 <= ft['input_timeout_s'] <= 240):
            raise ValueError('Invalid TorchFT settings; use the notebook defaults first')
        if len(set(settings['learning_rates'])) != 1:
            raise ValueError('DDP replicas must use the same learning rate')
        self.ml_config = saved['ml']
        self.ml_preparation = saved
        ft['fault_step'] = max(1, int(ft['fault_fraction']*self.ml_config['steps']))
        self.ft_config = ft
        self.versions = json.loads((self.local/'torchft-versions.json').read_text())
        rows = []
        for repeat in range(repeats):
            pair = [('off', False), ('on', True)]
            if repeat % 2:
                pair.reverse()
            for scenario, fault in [('healthy', False), ('loss', True)]:
                rows += [(f'{label}-{scenario}-r{repeat}', enabled, fault, True) for label, enabled in pair]
        return self.run_pilot(name, budget_s=budget_s, tasks=2*ft['shards_per_rank'],
            payload_bytes=8, timeout_s=timeout_s, observations=rows,
            keepalive_interval_ms=keepalive_interval_ms, keepalive_timeout_ms=keepalive_timeout_ms)

    def run_one(self, folder, enabled, fault, infrastructure, save, sample, workload):
        super().run_one(folder, enabled, fault, infrastructure, save, sample, workload)
        result = sample.get('result', {})
        if fault:
            # Event timestamps are support-host UTC; stop-request UTC comes from
            # the notebook host. Preserve that clock-skew limitation explicitly.
            requested = sample.get('stop_requested_unix')
            terminated = None
            if requested is not None and 'vm_terminated_monotonic' in sample:
                terminated = requested + (sample['vm_terminated_monotonic'] -
                                           sample['stop_requested_monotonic'])
            histories = result.get('events', [[], []])
            valid = requested is not None and sample.get('original_head_remained_off', False)
            evidence = []
            for history in histories:
                progress = [e for e in history if e['kind'] == 'progress']
                started = [e for e in history if e['kind'] == 'started']
                terminal = [e for e in history if e['kind'] in ('finished', 'failed')]
                before = [e for e in progress if requested is not None and e['server_unix_s'] <= requested]
                after = [e for e in progress if requested is not None and e['server_unix_s'] > requested]
                active = bool(started and before and terminated is not None and
                    before[-1]['step'] < workload['ml']['steps'] and
                    (not terminal or terminal[0]['server_unix_s'] > terminated))
                valid = valid and active
                evidence.append(dict(active_at_stop_request=active,
                    terminal_after_confirmed_vm_stop=bool(terminal and terminated is not None and
                        terminal[0]['server_unix_s'] > terminated),
                    step_before_stop=before[-1]['step'] if before else None,
                    first_progress_after_stop=after[0] if after else None,
                    note='Controller/support UTC comparison; event granularity and clock skew apply'))
            sample['training_at_failure'] = evidence
            sample['valid_head_loss_observation'] = bool(valid and len(histories) == 2)
            sample['recovered_correctly'] = bool(sample['valid_head_loss_observation'] and result.get('correct'))
            # Training may never have stopped. Do not call a post-fault poll a
            # measured failure-to-resumption latency in this continuous workload.
            sample['first_progress_observed_after_control_plane_recovery_upper_bound_s'] = sample.pop(
                'failure_to_resumption_upper_bound_s', None)
            if result.get('correct') and not sample['valid_head_loss_observation']:
                sample['status'] = 'invalid_failure_window'
        save()
