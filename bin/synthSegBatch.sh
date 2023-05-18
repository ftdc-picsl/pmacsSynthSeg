#!/bin/bash

module load miniconda/3-22.11
module load singularity/3.8.3

scriptPath=$(readlink -f "$0")
scriptDir=$(dirname "${scriptPath}")
# Repo base dir under which we find bin/ and containers/
repoDir=${scriptDir%/bin}

inputBIDS="/project/ftdc_volumetric/fw_bids"
maskBIDS="/project/ftdc_pipeline/data/synthstripT1w"
outputBIDS="/project/ftdc_pipeline/data/synthsegT1w"

function usage() {
  echo "Usage:
  $0 [-h] [-g 1/0] [-p 1/0] -i image_list
  "
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

function help() {
cat << HELP
  `usage`

  This is a wrapper script to submit images for processing. It assumes input from the BIDS dataset

  /project/ftdc_volumetric/fw_bids

  The image_list should be one per line, and relative to the BIDS dataset, eg

  sub-123456/ses-19970829x0214/anat/sub-123456_ses-19970829x0214_T1w.nii.gz

  Brain masks should exist in

    /project/ftdc_pipeline/synthstripT1w/

  The brain mask is only used to center the cropped FOV of synthseg. The input image is then cropped
  around the mask and resampled to 1mm isotropic resolution. This is the "SynthSeg space", which should
  be anatomically aligned with the native space, but with a different bounding box and spacing.

  Required args:

    -i image_list
        List of images to process, relative to the BIDS dataset

  Optional args:

    -g 1/0
        Use the GPU (default = 1).
    -p 0/1
        Output posteriors (default = 0).


  Output:

  Output is to the BIDS derivative dataset

    $outputBIDS

  Output files are prefixed with the input filename minus the BIDS extension, eg if the T1w input
  image is

    sub-123456_ses-20160429x0000_acq-mprage_T1w.nii.gz

  the output prefix will be sub-123456_ses-20160429x0000_acq-mprage_

  Output suffixes:

    desc-qc.tsv      - QC metrics for each label
    desc-volumes.tsv - Volumes for each label in the SynthSeg space (1mm isotropic)

    space-SynthSeg_T1w.nii.gz    - Input T1w in the SynthSeg space
    space-SynthSeg_dseg.nii.gz   - SynthSeg output in the SynthSeg space
    space-SynthSeg_dseg.tsv      - Label definitions

    space-orig_dseg.nii.gz - SynthSeg output in the original T1w space
    space-orig_dseg.tsv    - Label definitions

  If '-p 1' is specified, the following files will also be output:

    space-SynthSeg_probseg.nii.gz    - Posterior probabilities for each label in the SynthSeg space
    space-SynthSeg_posteriors.json   - JSON label map

    space-orig_probseg.nii.gz        - Posterior probabilities for each label in the original T1w space
    space-orig_posteriors.json       - JSON label map

HELP

}

imageList=""
useGPU=1
outputPosteriors=0

while getopts "g:i:p:h" opt; do
  case $opt in
    g) useGPU=$OPTARG;;
    i) imageList=$OPTARG;;
    p) outputPosteriors=$OPTARG;;
    h) help; exit 1;;
    \?) echo "Unknown option $OPTARG"; exit 2;;
    :) echo "Option $OPTARG requires an argument"; exit 2;;
  esac
done

shift $((OPTIND-1))

date=`date +%Y%m%d`

gpuBsubOpt=""
gpuScrptOpt=""

if [[ $useGPU -gt 0 ]]; then
  gpuBsubOpt='-gpu "num=1:mode=shared:mps=no:j_exclusive=no"'
  gpuScriptOpt="--gpu"
fi

posteriorsArg=""

if [[ $outputPosteriors -gt 0 ]]; then
  posteriorsArg="--posteriors"
fi

# Makes python output unbuffered, so we can tail the log file and see progress
# and errors in order
export PYTHONUNBUFFERED=1

bsub -cwd . $gpuBsubOpt -o "${outputBIDS}/logs/synthseg_${date}_%J.txt" \
    conda run -p /project/ftdc_pipeline/ftdc-picsl/miniconda/envs/ftdc-picsl-cp11 ${repoDir}/scripts/run_synthseg.py \
      --container ${repoDir}/containers/synthseg-mask-0.3.0.sif $gpuScriptOpt $posteriorsArg\
      --input-dataset $inputBIDS \
      --mask-dataset $maskBIDS \
      --output-dataset $outputBIDS \
      --anatomical-images $imageList
