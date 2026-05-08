module unload slurm
module load slurm/24.11.1
hash -r
cd ~/work/ray
git pull
srun -p batch --cpus-per-task=16 --mem=64G --time=02:00:00 --pty bash

