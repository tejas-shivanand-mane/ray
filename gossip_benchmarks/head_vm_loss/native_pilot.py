"""Bounded, owner-run GCP native head-loss pilot; no automatic cloud provisioning."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import sys
import threading
import time

from launcher import HeadVMLoss, PYTHON, BIN, ROOT, SHARED, NODE_UNIT, DRIVER_UNIT, parallel
from evidence import write_json

# Plain Python, reused only for the stable/beta capability query.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'selective_replication'))
from gcp.capabilities import stop_command

REDIS = 'ray-native-redis.service'
PROXY = 'ray-native-proxy.service'
GATE = 'ray-native-gate.service'
LOCK = '/var/lib/raybench/ray-headloss-lock'


class NativePilot(HeadVMLoss):
    driver_script = 'native_workload.py'
    max_observation_timeout_s = 300

    def workload_parameters(self):
        return {}

    def report_metadata(self):
        return {}

    def stop_after_observation(self, sample):
        return None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transport_events = []
        self.transport_lock = threading.Lock()

    def check_stop_capability(self):
        self.stop_args = stop_command()

    def gc(self, *args, **kwargs):
        # Retry only the explicit IAP connection-creation failure: no SSH
        # session was established. Never retry a timeout, generic SSH exit 255,
        # remote command failure, or VM mutation with an unknown outcome.
        check = kwargs.pop('check', True)
        end = time.monotonic() + kwargs.pop('timeout', 180)
        if getattr(self, 'deadline', None) is not None:
            end = min(end, self.deadline)
        transport = len(args) >= 3 and args[0] == 'compute' and args[1] in ('ssh', 'scp')
        for attempt in range(1, 4):
            remaining = end-time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Cloud command/session budget exhausted; inspect recorded outcomes')
            result = super().gc(*args, timeout=remaining, check=False, **kwargs)
            unreachable = (transport and result.returncode != 0 and
                "Error while connecting [4003:" in result.stderr and
                'Failed to connect to port 22' in result.stderr)
            delay = 2 * attempt
            retry = unreachable and attempt < 3 and end-time.monotonic() > delay + 1
            if unreachable:
                event = dict(unix_s=time.time(), operation=args[1],
                    target=str(args[2] if args[1] == 'ssh' else args[3]),
                    attempt=attempt, reason='IAP 4003: cannot connect to SSH port 22',
                    retry_in_s=delay if retry else None)
                with self.transport_lock:
                    self.transport_events.append(event)
                    with (self.local/'native-transport.jsonl').open('a') as stream:
                        stream.write(json.dumps(event)+'\n')
            if not retry:
                break
            print(f'IAP SSH connection failed; retry {attempt+1}/3 in {delay}s', flush=True)
            time.sleep(delay)
        if check and result.returncode:
            command = ['gcloud', *args, '--project='+self.project, '--quiet']
            hint = ('\nIAP could not reach SSH port 22. Check VM status, SSH service and '
                    'IAP firewall/connectivity; this is not a Ray recovery result.' if unreachable else '')
            raise RuntimeError(shlex.join(command)+'\n'+result.stderr[-4000:]+result.stdout[-4000:]+hint)
        return result

    def prepare(self):
        # Preparation has its own <=28 minute budget; no workload runs here.
        self.deadline = time.monotonic()+1680
        try:
            self.stop_args = stop_command()
            for name in self.names:
                self.describe(name)
                active = self.ssh(name, "systemctl list-units 'ray-selective-driver*' "
                    "'ray-native-*' --state=active,activating --no-legend").stdout
                if active.strip():
                    raise RuntimeError('Existing service must be inspected: '+active)
                if self.ssh(name, 'test ! -d /var/lib/raybench/ray-selective-lock', check=False).returncode:
                    raise RuntimeError('Selective experiment lock exists on '+name)
            # Base prepare checks image identity and moves NFS off the head.
            # Its stable-only capability check is superseded by stop_command().
            super().prepare()
            self.ssh(self.support, 'sudo DEBIAN_FRONTEND=noninteractive apt-get '
                '-o DPkg::Lock::Timeout=120 install -y -qq redis-server socat', timeout=300)
            for name in ('native_workload.py', 'native_probe.py', 'native_gate.py',
                         'native_ml_task.py', 'native_ml_workload.py',
                         'native_torchft_workload.py', 'evidence.py'):
                for vm in self.names:
                    self.scp(self.bundle / name, vm+':/tmp/'+name)
                    self.ssh(vm, f'sudo mkdir -p {ROOT} && sudo install -m 644 /tmp/{name} {ROOT}/{name}')
            self.ssh(self.support, 'command -v redis-server && command -v socat')
            write_json(self.local/'native-prepared.json', dict(
                helpers={p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                         for p in self.bundle.glob('native_*.py')}))
        except BaseException:
            self.ready = False
            raise
        finally:
            self.deadline = None

    def service(self, host, unit, argv, env=(), user='raybench', seconds=900):
        args = ['sudo', 'systemd-run', '--quiet', '--collect', '--unit='+unit,
            '--uid='+user, '--gid='+user, '--property=KillMode=control-group',
            '--property=TimeoutStopSec=10', '--property=Restart=no',
            '--property=RuntimeMaxSec='+str(seconds),
            '--setenv=PYTHONPATH='+ROOT,
            '--setenv=PATH='+BIN+':/usr/bin:/bin', '--setenv=RAY_USAGE_STATS_ENABLED=0',
            '--setenv=OMP_NUM_THREADS=1', '--setenv=OPENBLAS_NUM_THREADS=1']
        args += ['--setenv='+e for e in env]
        args += ['--setenv=RAY_'+key+'='+str(value)
                 for key, value in getattr(self, 'detection_config', {}).items()]
        self.ssh(host, shlex.join(args+argv))

    def stop_service(self, host, unit):
        self.ssh(host, f'if systemctl is-active --quiet {unit}; then sudo systemctl stop {unit}; fi')

    def proxy(self, target):
        self.stop_service(self.support, PROXY)
        self.service(self.support, PROXY, ['socat',
            f'TCP4-LISTEN:6379,bind={self.nodes[self.support]["ip"]},reuseaddr,fork',
            f'TCP4:{self.nodes[target]["ip"]}:6379'])

    def head_start(self, host, config, infrastructure):
        env = ['RAY_gcs_rpc_server_reconnect_timeout_s=600']
        command = [BIN+'/ray', 'start', '--block', '--head',
            '--port=6379', '--num-cpus=0', '--include-dashboard=false',
            '--node-ip-address='+self.nodes[host]['ip'], '--object-store-memory=536870912',
            '--system-config='+json.dumps(config)]
        if infrastructure:
            env += ['RAY_REDIS_ADDRESS='+self.nodes[self.support]['ip']+':6380']
            env += ['REDIS_PASSWORD_FILE='+self.redis_password_file]
            command = ['bash', '-c',
                'set -e; password=$(cat "$REDIS_PASSWORD_FILE"); exec "$@" --redis-password="$password"',
                'ray-native-head', *command]
        self.service(host, NODE_UNIT, command, env)
        # Probe direct GCS identity before redirecting clients.
        end = time.monotonic()+70
        while time.monotonic() < end:
            p = self.ssh(self.support, PYTHON+' '+ROOT+'/native_probe.py identity '+
                shlex.quote(self.nodes[host]['ip']+':6379'), check=False, timeout=15)
            if p.returncode == 0:
                return json.loads(p.stdout)
            time.sleep(1)
        raise TimeoutError('GCS did not become ready')

    def reset(self):
        obj = self.describe(self.head)
        if obj['status'] == 'TERMINATED':
            self.gc('compute', 'instances', 'start', self.head, '--zone='+self.zone, timeout=180)
            end = time.monotonic()+120
            while True:
                if self.ssh(self.head, 'true', check=False, timeout=20).returncode == 0:
                    break
                if time.monotonic() > end:
                    raise TimeoutError('Head boot timed out')
            self.ssh(self.head, f'mountpoint -q {SHARED} || sudo mount -t nfs -o vers=3,proto=tcp,hard '+
                f'{self.nodes[self.support]["ip"]}:{SHARED} {SHARED}')
        elif obj['status'] != 'RUNNING':
            raise RuntimeError('Head is in transitional state '+obj['status'])
        parallel(self._stop_node, self.names)
        for unit in (PROXY, REDIS, GATE):
            self.stop_service(self.support, unit)

    def capture_diagnostics(self, folder, sample):
        """Best effort, before reset/cleanup; missing diagnostics never imply success."""
        diagnostics = sample.setdefault('diagnostics', {})
        for host in self.names:
            if host == self.head and sample.get('instance_after', {}).get('status') == 'TERMINATED':
                diagnostics[host] = dict(unavailable='Head VM remains stopped')
                continue
            try:
                destination = folder+'/diagnostics/'+host
                result = self.ssh(host, 'timeout 12s sudo -u raybench '+PYTHON+' '+
                    ROOT+'/native_probe.py logs '+shlex.quote(destination), timeout=18)
                diagnostics[host] = json.loads(result.stdout)
            except Exception as exc:
                diagnostics[host] = dict(error=str(exc))
        address = self.nodes[self.support if sample['infrastructure'] else self.head]['ip']+':6379'
        try:
            result = self.ssh(self.support, 'timeout 12s '+PYTHON+' '+ROOT+
                '/native_probe.py identity '+shlex.quote(address), timeout=18)
            sample['gcs_at_observation_end'] = json.loads(result.stdout)
        except Exception as exc:
            sample['gcs_at_observation_end'] = dict(error=str(exc))

    def counters(self, names):
        result = {}
        for name in names:
            try:
                p = self.ssh(name, PYTHON+' '+ROOT+'/native_probe.py counters', timeout=15)
                result[name] = json.loads(p.stdout)
            except Exception as exc:
                result[name] = dict(error=str(exc))
        return result

    def run_one(self, folder, enabled, fault, infrastructure, save, sample, workload):
        self.reset()
        self.ssh(self.support, 'sudo -u raybench mkdir '+folder)
        config = dict(enable_recovery_succession=enabled,
            enable_recovery_witness_holder_baseline=enabled,
            enable_recovery_succession_profiling=True,
            recovery_succession_target_holder_count=2, recovery_succession_witness_count=2,
            recovery_baseline_perf_protect_every_n=1, enable_recovery_frontier=False,
            gcs_storage='redis' if infrastructure else 'memory',
            external_storage_namespace=Path(folder).name+'-'+self.run_id,
            gcs_rpc_server_reconnect_timeout_s=600, health_check_initial_delay_ms=1000,
            health_check_period_ms=1000, health_check_timeout_ms=3000,
            health_check_failure_threshold=3)
        config.update(self.detection_config)
        if infrastructure:
            # Keep credentials outside the downloadable suite directory. Both
            # heads read the same owner-only file from surviving w4-backed NFS.
            credentials = SHARED+'/credentials'
            self.redis_password_file = credentials+'/'+hashlib.sha256(folder.encode()).hexdigest()
            self.ssh(self.support, f'sudo install -d -m 700 -o raybench -g raybench {credentials}')
            generate = (
                'import os,secrets; fd=os.open('+repr(self.redis_password_file)+
                ',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600); '
                'f=os.fdopen(fd,"w"); f.write(secrets.token_hex(32)); '
                'f.flush(); os.fsync(f.fileno()); f.close()')
            self.ssh(self.support, 'sudo -u raybench '+PYTHON+' -c '+shlex.quote(generate))
            self.service(self.support, REDIS, ['bash', '-c',
                'set -e; password=$(cat "$REDIS_PASSWORD_FILE"); exec "$@" --requirepass "$password"',
                'ray-native-redis', 'redis-server', '--bind', self.nodes[self.support]['ip'],
                '--port', '6380', '--protected-mode', 'yes', '--save', '', '--appendonly', 'yes',
                '--appendfsync', 'always', '--dir', folder],
                env=['REDIS_PASSWORD_FILE='+self.redis_password_file])
            # Private bind alone does not bypass modern Redis protected mode.
            ping_command = ('set -e; export REDISCLI_AUTH="$(cat '+
                shlex.quote(self.redis_password_file)+')"; exec redis-cli -h '+
                shlex.quote(self.nodes[self.support]['ip'])+' -p 6380 ping')
            end = time.monotonic()+30
            while True:
                ping = self.ssh(self.support, 'sudo -u raybench bash -c '+
                    shlex.quote(ping_command), check=False, timeout=10)
                sample['redis_probe'] = dict(returncode=ping.returncode,
                    stdout=ping.stdout[-4000:], stderr=ping.stderr[-4000:])
                save()
                if ping.returncode == 0 and ping.stdout.strip() == 'PONG':
                    break
                if any(word in ping.stdout+ping.stderr for word in ('DENIED', 'WRONGPASS', 'NOAUTH')):
                    raise RuntimeError('Redis authentication/protected-mode failure: '+ping.stdout+ping.stderr)
                if time.monotonic() > end:
                    raise TimeoutError('Dedicated Redis did not become ready; last probe: '+repr(sample['redis_probe']))
                time.sleep(1)
        before = self.head_start(self.head, config, infrastructure)
        if infrastructure:
            self.proxy(self.head)
        address = self.nodes[self.support if infrastructure else self.head]['ip']+':6379'
        for name in [self.support, *self.trainers]:
            self.service(name, NODE_UNIT, [BIN+'/ray', 'start', '--block', '--address='+address,
                '--node-ip-address='+self.nodes[name]['ip'], '--num-cpus=4',
                '--object-store-memory=536870912'], ['RAY_gcs_rpc_server_reconnect_timeout_s=600'])
        end = time.monotonic()+70
        while True:
            p = self.ssh(self.support, PYTHON+' '+ROOT+'/native_probe.py identity '+address, timeout=15)
            identity = json.loads(p.stdout)
            if len([n for n in identity['nodes'] if n['alive']]) == 4:
                break
            if time.monotonic() > end:
                raise TimeoutError('Four Ray nodes did not register')
            time.sleep(1)
        cfg = dict(folder=folder, enabled=enabled, fault=fault, infrastructure=infrastructure, address=address,
            head_ip=self.nodes[self.head]['ip'], support_ip=self.nodes[self.support]['ip'],
            replacement_ip=self.nodes[self.replacement]['ip'],
            executor_ips=[self.nodes[n]['ip'] for n in self.trainers],
            gate_url='http://'+self.nodes[self.support]['ip']+':8765',
            detection_config=self.detection_config,
            **workload)
        self._upload_json(cfg, folder+'/native-config.json')
        self.service(self.support, GATE, [PYTHON, ROOT+'/native_gate.py', folder+'/native-config.json'])
        health = ('from native_gate import request; print(request('+repr(cfg['gate_url'])+", 'health'))")
        end = time.monotonic()+30
        while True:
            response = self.ssh(self.support, 'cd '+ROOT+' && '+PYTHON+' -c '+shlex.quote(health),
                                check=False, timeout=10)
            if response.returncode == 0:
                break
            if time.monotonic() > end:
                raise RuntimeError('HTTP task gate failed: '+response.stderr)
            time.sleep(.5)
        for host in self.trainers:
            self.ssh(host, 'cd '+ROOT+' && '+PYTHON+' -c '+shlex.quote(health), timeout=15)
        sample.update(native_config=config, before=identity,
            counters_before=self.counters(self.names), workload=cfg)
        sample['workload_dispatch_monotonic'] = time.monotonic()
        save()
        self.service(self.support, DRIVER_UNIT, [PYTHON, ROOT+'/'+self.driver_script, folder+'/native-config.json'], seconds=cfg['timeout_s']+30)
        end = time.monotonic()+cfg['timeout_s']
        fault_done = False
        while time.monotonic() < end:
            result = self._remote_json(folder+'/result.json')
            if result:
                sample['result'] = result
                sample['status'] = result['status']
                break
            ready = self._remote_json(folder+'/ready.json')
            if ready and not fault_done:
                sample['gate'] = ready
                save()
                if fault:
                    self._stop_vm(sample, save)
                    self._head_off()
                    restored = self.head_start(self.replacement, config, infrastructure)
                    sample['replacement_direct_snapshot'] = restored
                    if any(restored[k] != before[k] for k in ('cluster_id', 'session')):
                        raise RuntimeError('Replacement changed GCS identity; endpoint not switched')
                    self.proxy(self.replacement)
                    sample['endpoint_switched_monotonic'] = time.monotonic()
                    self._head_off()
                self._upload_json(dict(proceed=True), folder+'/continue.json')
                fault_done = True
                save()
            progress = self._remote_json(folder+'/progress.json')
            resumed = progress and (progress.get('training_resumed') or
                len(progress['reads']) > workload.get('pre_failure_reads', 1))
            if fault and fault_done and resumed and 'resumed_monotonic' not in sample:
                sample['resumed_monotonic'] = time.monotonic()
                sample['failure_to_resumption_upper_bound_s'] = time.monotonic()-sample['stop_requested_monotonic']
                save()
            time.sleep(.5)
        else:
            sample['status'] = 'timeout'
        sample['completion_observed_monotonic'] = time.monotonic()
        if fault_done and fault:
            self._head_off()
            sample['original_head_remained_off'] = True
            if sample.get('result', {}).get('correct') and 'resumed_monotonic' not in sample:
                sample['failure_to_resumption_upper_bound_s'] = time.monotonic()-sample['stop_requested_monotonic']
            sample['failure_to_outcome_upper_bound_s'] = time.monotonic()-sample['stop_requested_monotonic']
            if sample.get('result', {}).get('correct'):
                sample['failure_to_completion_upper_bound_s'] = sample['failure_to_outcome_upper_bound_s']
        sample['counters_after'] = self.counters([n for n in self.names if not (fault and n == self.head)])
        sample['missing_head_final_counters'] = fault
        save()

    def _stop_vm(self, record, save):
        # Capability selection was checked in prepare; record command and async operation.
        record['instance_before'] = self.describe(self.head)
        if record['instance_before']['status'] != 'RUNNING':
            raise RuntimeError('Head was not running')
        record['stop_requested_monotonic'] = time.monotonic()
        record['stop_requested_unix'] = time.time()
        record['stop_command'] = self.stop_args
        save()
        p = self.gc(*self.stop_args, self.head, '--zone='+self.zone,
            '--no-graceful-shutdown', '--async', '--format=json')
        record['stop_operation'] = json.loads(p.stdout)
        save()
        end = time.monotonic()+180
        while time.monotonic() < end:
            obj = self.describe(self.head)
            if obj['status'] == 'TERMINATED':
                record['instance_after'] = obj
                record['vm_terminated_monotonic'] = time.monotonic()
                save()
                return
            time.sleep(1)
        raise TimeoutError('Head termination unconfirmed')

    def run_pilot(self, name, budget_s=1680, *, tasks=16,
                  payload_bytes=1024*1024, timeout_s=300, observations=None,
                  keepalive_interval_ms=10000, keepalive_timeout_ms=10000):
        self._check_name(name)
        if not self.ready or not 60 <= budget_s <= 1680:
            raise ValueError('Prepare first; budget must be 60..1680 seconds')
        if (type(tasks) is not int or not 2 <= tasks <= 32 or
                type(payload_bytes) is not int or not 8 <= payload_bytes <= 4*1024*1024 or
                type(timeout_s) is not int or not 60 <= timeout_s <= self.max_observation_timeout_s):
            raise ValueError('Invalid task count, payload size, or observation timeout')
        if any(type(v) is not int or not 1000 <= v <= 15000
               for v in (keepalive_interval_ms, keepalive_timeout_ms)):
            raise ValueError('Diagnostic keepalive settings must be 1000..15000 ms')
        self.detection_config = dict(grpc_client_keepalive_time_ms=keepalive_interval_ms,
                                     grpc_client_keepalive_timeout_ms=keepalive_timeout_ms)
        if observations is None:
            observations = [
                ('plain-healthy', False, False, False),
                ('off-healthy', False, False, True), ('on-healthy', True, False, True),
                ('off-loss', False, True, True), ('on-loss', True, True, True)]
        observations = [list(row) for row in observations]
        if not observations or len(observations) > 10:
            raise ValueError('Use 1..10 predeclared observations')
        for row in observations:
            if len(row) != 4 or any(type(flag) is not bool for flag in row[1:]):
                raise ValueError('Each observation is (label, enabled, fault, infrastructure)')
            self._check_name(row[0])
            if row[2] and not row[3]:
                raise ValueError('Head replacement requires Redis/proxy infrastructure')
        if len({row[0] for row in observations}) != len(observations):
            raise ValueError('Observation labels must be unique')
        workload = dict(tasks=tasks, payload_bytes=payload_bytes, timeout_s=timeout_s)
        workload.update(self.workload_parameters())
        settings = dict(workload=workload, observations=observations,
                        detection_config=self.detection_config, gate='off-head-http-v1')
        for vm in self.names:
            self.describe(vm)  # Recheck ownership and instance IDs on every resume.
        directory = self.local / name
        directory.mkdir(exist_ok=True)
        report_path = directory/'native-report.json'
        fresh = not report_path.exists()
        report = json.loads(report_path.read_text()) if not fresh else dict(
            samples=[], failure_model='head VM stopped, disk inaccessible and never reattached',
            recovery='same GCS session via off-head Redis and stable TCP proxy; no job restart',
            topology=dict(head=self.head, replacement=self.replacement, support=self.support, executors=self.trainers),
            extra_resources='one reserved replacement VM; Redis/proxy/HTTP gates/NFS/driver share support VM',
            environment=json.loads((self.local/'environment.json').read_text()),
            interpretation='single diagnostic replicate; not steady-state throughput or production HA')
        if not fresh and report.get('settings') != settings:
            raise ValueError('Settings changed or legacy report: use a new suite name')
        report['settings'] = settings
        report.pop('paused_reason', None)
        report.update(self.report_metadata())
        report['planned_observations'] = len(observations)
        hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in self.bundle.glob('native_*.py')}
        prepared = json.loads((self.local/'native-prepared.json').read_text())
        if prepared['helpers'] != hashes:
            raise ValueError('Preparation is missing/stale; prepare the current helpers first')
        if report.get('helper_hashes', hashes) != hashes:
            raise ValueError('Helpers changed; use a new suite name')
        if report.get('blocked_reason'):
            raise ValueError(report['blocked_reason'])
        report['helper_hashes'] = hashes
        def save():
            write_json(report_path, report)
        save()
        # Remote lock prevents concurrent controllers, including on another notebook host.
        self.ssh(self.support, 'sudo -u raybench mkdir '+LOCK)
        self.deadline = time.monotonic()+budget_s
        try:
            if fresh:
                self.ssh(self.support, f'sudo -u raybench mkdir -p {SHARED}/{self.run_id}')
                # Exclusive remote suite creation protects evidence even if the
                # notebook host lost its local report. Never overwrite remote rows.
                self.ssh(self.support, f'sudo -u raybench mkdir {SHARED}/{self.run_id}/{name}')
            for label, enabled, fault, infra in observations:
                if any(r['label'] == label for r in report['samples']):
                    continue  # Failed/interrupted observations are never silently overwritten.
                if self.deadline-time.monotonic() < timeout_s+120:
                    report['paused_reason'] = 'Insufficient time for the workload plus two-minute setup reserve'
                    break
                sample = dict(label=label, enabled=enabled, fault=fault, infrastructure=infra, status='running')
                report['samples'].append(sample)
                save()
                print(label, flush=True)
                transport_start = len(self.transport_events)
                try:
                    self.run_one(f'{SHARED}/{self.run_id}/{name}/{label}', enabled, fault, infra, save, sample, workload)
                except Exception as exc:
                    sample.update(status='infrastructure_or_setup_error', error=str(exc))
                    save()
                    try:
                        log = self.ssh(self.support,
                            f'sudo journalctl -u {REDIS} -u {NODE_UNIT} -u {DRIVER_UNIT} '
                            '--no-pager -n 200', timeout=15)
                        log_path = directory/(label+'-startup-failure.log')
                        log_path.write_text(log.stdout)
                        sample['failure_journal'] = str(log_path)
                        print(log.stdout, flush=True)
                    except Exception as diagnostic_error:
                        sample['diagnostic_error'] = str(diagnostic_error)
                    save()
                    # Unknown cloud/start outcomes must be inspected, not automatically retried.
                    raise
                finally:
                    self.capture_diagnostics(f'{SHARED}/{self.run_id}/{name}/{label}', sample)
                    sample['transport_events'] = self.transport_events[transport_start:]
                    save()
                for vm in self.names:
                    if fault and vm == self.head:
                        continue
                    try:
                        log = self.ssh(vm, f'sudo journalctl -u {NODE_UNIT} -u {DRIVER_UNIT} '
                            f'-u {REDIS} -u {PROXY} -u {GATE} --no-pager -n 400', timeout=15)
                        (directory/(label+'-'+vm+'.log')).write_text(log.stdout)
                    except Exception as exc:
                        sample.setdefault('journal_errors', {})[vm] = str(exc)
                    sample['transport_events'] = self.transport_events[transport_start:]
                    save()
                print(label+': '+sample['status'], flush=True)
                stop_reason = self.stop_after_observation(sample)
                if stop_reason:
                    report['blocked_reason'] = report['paused_reason'] = stop_reason
                    save()
                    print(stop_reason, flush=True)
                    break
            report['finished_matrix'] = len(report['samples']) == len(observations) and all(r['status'] != 'running' for r in report['samples'])
        finally:
            # Reserve at most 90 seconds beyond the work budget for fencing. If it
            # fails keep the lock; never restart a failed head during cleanup.
            self.deadline = time.monotonic()+90
            errors = []
            for node in self.names:
                try:
                    if self.describe(node)['status'] == 'RUNNING':
                        self._stop_node(node)
                except Exception as exc:
                    errors.append(str(exc))
            for unit in (PROXY, REDIS, GATE):
                try:
                    self.stop_service(self.support, unit)
                except Exception as exc:
                    errors.append(str(exc))
            report['cleanup_errors'] = errors
            save()
            try:
                if not errors:
                    try:
                        self.ssh(self.support, 'sudo -u raybench rmdir '+LOCK)
                    except Exception as exc:
                        errors.append('Could not release suite lock: '+str(exc))
                        report['cleanup_errors'] = errors
                        save()
            finally:
                self.deadline = None
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['inventory', 'prepare', 'run', 'download'])
    parser.add_argument('--state', required=True)
    parser.add_argument('--suite', default='native-head-v5')
    args = parser.parse_args()
    pilot = NativePilot(args.state)
    if args.action == 'inventory':
        print(json.dumps(pilot.inventory(), indent=2))
    elif args.action == 'prepare':
        pilot.prepare()
    elif args.action == 'download':
        print(pilot.download(args.suite))
    else:
        # Reload the preparation manifest; no hidden prepare/reset on resume.
        env = json.loads((pilot.local/'environment.json').read_text())
        pilot.nodes = env['nodes']
        pilot.stop_args = stop_command()
        pilot.ready = True
        print(json.dumps(pilot.run_pilot(args.suite), indent=2))


if __name__ == '__main__':
    main()
