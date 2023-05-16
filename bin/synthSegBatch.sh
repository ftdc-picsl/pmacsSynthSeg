#!/bin/bash

module load miniconda/3-22.11
module load singularity/3.8.3

scriptPath=$(readlink -f "$0")
scriptDir=$(dirname "${scriptPath}")
# Repo base dir under which we find bin/ and containers/
repoDir=${scriptDir%/bin}

inputBIDS="/project/ftdc_volumetric/fw_bids"

outputBIDS=/project/ftdc_pipeline/data/synthsegT1w

function usage() {
  echo "Usage:
  $0 [-h] [-g 1/0] -i image_list

  This is a wrapper script to submit images for processing. It assumes input from the BIDS dataset

  /project/ftdc_volumetric/fw_bids

  The image_list should be one per line, and relative to the BIDS dataset, eg

  sub-123456/ses-19970829x0214/anat/sub-123456_ses-19970829x0214_T1w.nii.gz

  Brain masks should exist in

    /project/ftdc_pipeline/synthstripT1w/

  The brain mask is only used to center the cropped FOV of synthseg.

  Output is to

    outputBIDS

"
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

imageList=""
useGPU=1

while getopts "g:i:h" opt; do
  case $opt in
    g) useGPU=$OPTARG;;
    i) imageList=$OPTARG;;
    h) usage; exit 1;;
    \?) echo "Unknown option $OPTARG"; exit 2;;
    :) echo "Option $OPTARG requires an argument"; exit 2;;
  esac
done

shift $((OPTIND-1))

imageList=$1

date=`date +%Y%m%d`

gpuBsubOpt=""
gpuScrptOpt=""

if [[ $useGPU -gt 0]]; then
  gpuBsubOpt='-gpu "num=1:mode=shared:mps=no:j_exclusive=no"'
  gpuScriptOpt="--gpu"
fi

bsub -cwd . $gpuOpt -o "/project/ftdc_pipeline/data/synthseg/logs/synthseg_${date}_%J.txt" \
    conda run -n /project/ftdc_pipeline/ftdc-picsl/miniconda/envs/ftdc-picsl-cp11 ${repoDir}/scripts/run_synthseg.py \
      --container ${repoDir}/containers/synthseg-mask-0.2.0.sif \
      --input-dataset $inputBIDS \
      --mask-dataset $maskBIDS \
      --output-dataset \
      --anatomical-images $imageList