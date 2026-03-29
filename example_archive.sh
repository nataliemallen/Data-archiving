#!/bin/bash
#SBATCH --job-name=archive
#SBATCH -A dewoody
#SBATCH -t 12-00:00:00 
#SBATCH -p cpu
#SBATCH -n 128
#SBATCH -e %x_%j.err
#SBATCH -o %x_%j.out
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=allen715@purdue.edu

module load anaconda

python archive_create.py -d "final whale bam files" -l to_archive.txt --tags whales

# move tarball and readme to fortress
# hsi put 20260329_221654_final_bam_and_ref_genome_files.tar.gz /home/allen715
