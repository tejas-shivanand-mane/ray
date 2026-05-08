module unload slurm
module load slurm/24.11.1
hash -r
srun -p batch --cpus-per-task=16 --mem=64G --time=02:00:00 --pty bash
conda activate ray-dev
cd ~/work/ray
git pull
pip install -e python/ 2>&1
