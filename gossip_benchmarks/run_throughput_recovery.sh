#!/bin/bash
#SBATCH --job-name=ray-tp-recovery
#SBATCH --output=ray-tp-recovery-%j.log
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=16G
#SBATCH --time=00:30:00
#SBATCH --partition=short

source /usr/share/Modules/init/bash
module load gcc/12.2.0
export LD_LIBRARY_PATH=/opt/linux/rocky/8.x/x86_64/pkgs/gcc/12.2.0/lib64:$LD_LIBRARY_PATH
source /opt/linux/rhel/8.x/x86_64/pkgs/miniconda3/py39_4.12.0/etc/profile.d/conda.sh
conda activate ray-dev
export PATH=/rhome/tmane002/.conda/envs/ray-dev/bin:$PATH

nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)
head_node=${nodes_array[0]}
worker_a=${nodes_array[1]}
worker_b=${nodes_array[2]}

head_ip=$(srun --cpu-bind=none --nodes=1 --ntasks=1 \
    -w "$head_node" hostname -I | awk '{print $1}')
worker_a_ip=$(srun --cpu-bind=none --nodes=1 --ntasks=1 \
    -w "$worker_a" hostname -I | awk '{print $1}')
worker_b_ip=$(srun --cpu-bind=none --nodes=1 --ntasks=1 \
    -w "$worker_b" hostname -I | awk '{print $1}')

port=6379
ip_head=$head_ip:$port
export RAY_ADDRESS=$ip_head

echo "Head:     $head_node ($head_ip)"
echo "Worker A: $worker_a ($worker_a_ip)"
echo "Worker B: $worker_b ($worker_b_ip)"

rm -f /rhome/tmane002/ready_to_kill.txt

# Shared NFS log directory — all nodes write here directly
LOG_BASE=/rhome/tmane002/ray_logs_$SLURM_JOBID
mkdir -p $LOG_BASE

# Start Ray head (0 CPUs)
srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$head_node" \
    ray start --head \
    --node-ip-address=$head_ip \
    --port=$port \
    --num-cpus=0 \
    --temp-dir=$LOG_BASE \
    --block &
sleep 15

# Start worker_a — owner lives here
srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$worker_a" \
    ray start --address=$ip_head \
    --node-ip-address=$worker_a_ip \
    --num-cpus=8 \
    --resources='{"worker_a": 1}' \
    --temp-dir=$LOG_BASE \
    --block &
sleep 5

# Start worker_b — compute tasks run here
srun --cpu-bind=none --nodes=1 --ntasks=1 -w "$worker_b" \
    ray start --address=$ip_head \
    --node-ip-address=$worker_b_ip \
    --num-cpus=8 \
    --resources='{"worker_b": 8}' \
    --temp-dir=$LOG_BASE \
    --block &
sleep 5

echo "Cluster ready"

# Kill watcher — kills worker_a once signal file appears
(
    while true; do
        if [ -f /rhome/tmane002/ready_to_kill.txt ]; then
            echo "Kill signal received — stopping worker_a ($worker_a)..."
            srun --cpu-bind=none --nodes=1 --ntasks=1 \
                -w "$worker_a" ray stop --force
            echo "worker_a stopped"
            rm -f /rhome/tmane002/ready_to_kill.txt
            break
        fi
        sleep 0.1
    done
) &
WATCHER_PID=$!

OUTPUT="/rhome/tmane002/results/gossip_throughput_recovery_gossip.csv"
rm -f $OUTPUT

python /rhome/tmane002/work/ray/gossip_benchmarks/throughput_recovery_benchmark.py \
    --system gossip \
    --output $OUTPUT

kill $WATCHER_PID 2>/dev/null

echo ""
echo "Results:"
cat $OUTPUT

echo ""
echo "Logs saved to: $LOG_BASE/session_*/logs/"