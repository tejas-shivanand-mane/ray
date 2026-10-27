"""Owner-run ML extension of the measured native head-VM-loss pilot."""
import hashlib
import json
import math
from pathlib import Path
import time

from evidence import write_json
from launcher import ROOT, SHARED, PYTHON
from native_pilot import NativePilot, LOCK


class NativeMLPilot(NativePilot):
    driver_script = 'native_ml_workload.py'

    def workload_parameters(self):
        return dict(ml=self.ml_config, pre_failure_reads=0)

    def report_metadata(self):
        return dict(ml_preparation=self.ml_preparation,
            interpretation='real Fashion-MNIST CNN training trials; cold task startup included; '
                'single-thread CPU, fixed optimizer steps; not DDP, TorchFT, or checkpoint recovery')

    def prepare_ml(self, name, settings):
        """Download real inputs once, copy to executors, and calibrate fixed work.

        This executes only when the owner calls the notebook cell. No arm extends
        its own training to meet a time floor: all arms execute identical steps.
        """
        self._check_name(name)
        if not self.ready:
            raise ValueError('Run cloud.prepare() before ML preparation')
        if (not 60 <= settings['minimum_training_s'] < settings['target_training_s'] <= 120 or
                not 5 <= settings['calibration_s'] <= 20 or
                type(settings['batch_size']) is not int or not 32 <= settings['batch_size'] <= 256 or
                type(settings['train_examples']) is not int or not 2000 <= settings['train_examples'] <= 60000 or
                type(settings['validation_examples']) is not int or not 500 <= settings['validation_examples'] or
                settings['train_examples']+settings['validation_examples'] > 60000 or
                type(settings['test_examples']) is not int or not 500 <= settings['test_examples'] <= 10000 or
                type(settings['seed']) is not int or not 0 <= settings['seed'] < 2**31 or
                len(settings['learning_rates']) != 2 or
                any(not 0 < v <= .01 for v in settings['learning_rates']) or
                not 0 < settings['minimum_accuracy'] < 1):
            raise ValueError('Invalid ML settings; see notebook limits')
        prepared = json.loads((self.local/'native-prepared.json').read_text())
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.bundle.glob('native_*.py')}
        if prepared['helpers'] != hashes:
            raise ValueError('Helpers changed; run preparation first')
        local_manifest = self.local/('ml-preparation-'+name+'.json')
        if local_manifest.exists():
            saved = json.loads(local_manifest.read_text())
            if saved['settings'] != settings or saved['helpers'] != hashes or saved['nodes'] != self.nodes:
                raise ValueError('ML preparation changed: use a new suite name')
            print('Reusing fixed calibration:', local_manifest, flush=True)
            return saved
        if (self.local/name/'native-report.json').exists():
            raise ValueError('Existing suite is missing calibration; do not overwrite it')
        self.deadline = time.monotonic()+1680
        try:
            if self.ssh(self.support, 'test ! -d '+LOCK, check=False).returncode:
                raise RuntimeError('A controller lock exists; do not calibrate an active cluster')
            for vm in self.names:
                active = self.ssh(vm, "systemctl list-units 'ray-headloss-*' 'ray-native-*' "
                                  '--state=active,activating --no-legend').stdout
                if active.strip():
                    raise RuntimeError('Stop/inspect active experiment services before calibration: '+active)
            remote = SHARED+'/'+self.run_id+'/ml-inputs/'+name
            # Create all ancestors as raybench; a root-owned run directory would
            # prevent the later suite creation when using a fresh cluster.
            self.ssh(self.support, 'sudo -u raybench mkdir -p '+remote)
            download = dict(settings, data_path=remote+'/fashion.pt', download_root=remote+'/download')
            self._upload_json(download, remote+'/download-config.json')
            self.ssh(self.support, 'timeout 600s sudo -u raybench '+PYTHON+' '+ROOT+
                '/native_ml_task.py prepare '+remote+'/download-config.json '+remote+'/dataset.json', timeout=630)
            dataset = self._remote_json(remote+'/dataset.json')
            local_data = ROOT+'/ml-inputs/'+self.run_id+'/'+name+'/fashion.pt'
            base = dict(data_path=local_data, input_sha256=dataset['input_sha256'],
                        batch_size=settings['batch_size'], seed=settings['seed'],
                        learning_rates=settings['learning_rates'], steps=100000,
                        gate_step=1, calibration_s=settings['calibration_s'])
            calibrations = []
            for index, vm in enumerate(self.trainers):
                self.ssh(vm, 'sudo install -d -o raybench -g raybench '+str(Path(local_data).parent)+
                    ' && sudo -u raybench cp '+remote+'/fashion.pt '+local_data, timeout=120)
                cfg = dict(base, trial_index=index)
                self._upload_json(cfg, remote+f'/calibration-{index}.json')
                self.ssh(vm, 'timeout 90s sudo -u raybench env OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 '+
                    PYTHON+' '+ROOT+'/native_ml_task.py calibrate '+remote+f'/calibration-{index}.json '+
                    remote+f'/calibration-result-{index}.json', timeout=110)
                measured = self._remote_json(remote+f'/calibration-result-{index}.json')
                measured['vm'] = vm
                calibrations.append(measured)
            # The fastest worker determines a shared count. Headroom reduces the
            # chance of a faster measured arm dropping below 60s; the floor is
            # still checked against actual training time, not this prediction.
            seconds_per_step = min(c['seconds_per_step'] for c in calibrations)
            steps = math.ceil(settings['target_training_s']*1.15/seconds_per_step)
            if not 20 <= steps <= 20000:
                raise ValueError('Calibration produced unsupported step count: '+str(steps))
            ml = dict(data_path=local_data, input_sha256=dataset['input_sha256'],
                batch_size=settings['batch_size'], seed=settings['seed'],
                learning_rates=settings['learning_rates'], steps=steps, gate_step=max(1, steps//10),
                minimum_training_s=settings['minimum_training_s'], minimum_accuracy=settings['minimum_accuracy'])
            saved = dict(settings=settings, ml=ml, dataset=dataset, calibrations=calibrations,
                         helpers=hashes, nodes=self.nodes,
                         predicted_training_s=[steps*c['seconds_per_step'] for c in calibrations])
            write_json(local_manifest, saved)
            print('Fixed optimizer steps per trial:', steps, flush=True)
            print('Calibration predictions (not experiment measurements):', saved['predicted_training_s'], flush=True)
            return saved
        finally:
            self.deadline = None

    def run_ml(self, name, settings, *, repeats=1, budget_s=1680,
               keepalive_interval_ms=10000, keepalive_timeout_ms=10000):
        self._check_name(name)
        if type(repeats) is not int or not 1 <= repeats <= 2:
            raise ValueError('Use one or two full matched repetitions per resumable suite')
        saved = json.loads((self.local/('ml-preparation-'+name+'.json')).read_text())
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.bundle.glob('native_*.py')}
        if saved['settings'] != settings or saved['nodes'] != self.nodes or saved['helpers'] != hashes:
            raise ValueError('Settings/cluster/helpers changed; prepare a new suite')
        self.ml_config = saved['ml']
        self.ml_preparation = saved
        observations = []
        for repeat in range(repeats):
            healthy = [('off-healthy', False, False, True), ('on-healthy', True, False, True)]
            if repeat % 2:
                healthy.reverse()
            rows = [('plain-healthy', False, False, False), *healthy,
                    ('off-loss', False, True, True), ('on-loss', True, True, True)]
            observations += [(label+f'-r{repeat}', enabled, fault, infra) for label, enabled, fault, infra in rows]
        return self.run_pilot(name, budget_s=budget_s, tasks=2, payload_bytes=8,
            timeout_s=300, observations=observations,
            keepalive_interval_ms=keepalive_interval_ms, keepalive_timeout_ms=keepalive_timeout_ms)
