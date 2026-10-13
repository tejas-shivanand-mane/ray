#!/bin/bash
#SBATCH --job-name=ray-ml-recovery
#SBATCH --output=ray-ml-recovery-%A_%a.log
#SBATCH --nodes=5
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=03:00:00
#SBATCH --partition=batch
#SBATCH --array=0-3%1

set -eo pipefail
source /usr/share/Modules/init/bash
module load gcc/12.2.0
export LD_LIBRARY_PATH="/opt/linux/rocky/8.x/x86_64/pkgs/gcc/12.2.0/lib64:${LD_LIBRARY_PATH:-}"
source /opt/linux/rhel/8.x/x86_64/pkgs/miniconda3/py39_4.12.0/etc/profile.d/conda.sh
conda activate /rhome/tmane002/.conda/envs/ray-dev
export PATH="/rhome/tmane002/.conda/envs/ray-dev/bin:$PATH"
export RAY_TRAIN_V2_ENABLED=1
export RAY_USAGE_STATS_ENABLED=0
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
unset RAY_ADDRESS
set -u

REPO="${REPO:-/rhome/tmane002/work/ray}"
RESULTS="${RESULTS:-/rhome/tmane002/results}"
REPEATS="${REPEATS:-3}"
STEPS="${STEPS:-300}"
FAULT_STEP="${FAULT_STEP:-239}"
WIDTH="${WIDTH:-2048}"
BATCH_SIZE="${BATCH_SIZE:-256}"
FAULT_RANK="${FAULT_RANK:-0}"
cd "$REPO"
intervals=(30 60 120 120)
index="${SLURM_ARRAY_TASK_ID:-0}"
interval="${intervals[$index]}"
comparison=ordinary-ray
if [[ "$index" == 3 ]]; then comparison=torchft-input; fi
# Explicit override is useful for the short smoke allocation.
interval="${CHECKPOINT_EVERY:-$interval}"
run_dir="$RESULTS/ucr-${SLURM_ARRAY_JOB_ID:-$SLURM_JOB_ID}-$index"
mkdir -p "$run_dir"
report="$run_dir/$comparison-cp$interval.json"

# Runs directly in the batch step on the head host. The Python backend launches
# Ray steps with --overlap so long-lived head/worker steps cannot block commands.
# Do not wrap this driver in a worker-node srun.
python gossip_benchmarks/run_step_input_experiment.py \
    --slurm --comparison "$comparison" --repeats "$REPEATS" \
    --steps "$STEPS" --batch-size "$BATCH_SIZE" --model-width "$WIDTH" \
    --checkpoint-every "$interval" --fault-step "$FAULT_STEP" \
    --failed-rank "$FAULT_RANK" --startup-delay-s 0 --timeout-s 900 \
    --output "$report"
python gossip_benchmarks/plot_step_input_experiment.py \
    "$report" --output "$run_dir/$comparison-cp$interval.png"
echo "Report and PNG/PDF plots: $run_dir"
