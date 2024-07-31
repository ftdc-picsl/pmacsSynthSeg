#!/bin/bash

module load miniconda/3-22.11
module load singularity/3.8.3

scriptPath=$(readlink -f "$0")
scriptDir=$(dirname "${scriptPath}")
# Repo base dir under which we find bin/ and containers/
repoDir=${scriptDir%/bin}

inputBIDS="/project/ftdc_volumetric/fw_bids"
maskBIDS="/project/ftdc_pipeline/data/synthstripT1w"
outputBIDS=""

function usage() {
  echo "Usage:
  $0 [-h] [-a 0/1] [-g 1/0] [-p 0/1] [-i input_dataset] [-m mask_dataset] -o output_dataset subject session
  "
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

function help() {
cat << HELP
  `usage`

  This is a wrapper script to submit a single session for processing.

  A brain mask is required, it is used to center the cropped FOV of synthseg. The input image is then cropped
  around the mask and resampled to 1mm isotropic resolution. This is the "SynthSeg space", which should
  be anatomically aligned with the native space, but with a different bounding box and spacing.

  Required args:

    -o output dataset
        Output BIDS dataset.


  Optional args:

    -a 0/1
        Output in antsct format (default = 0).
    -g 1/0
        Use the GPU (default = 1).
    -i input dataset
        Input BIDS dataset (default = $inputBIDS).
    -m mask dataset
        BIDS dataset containing brain masks (default = $maskBIDS).
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

  If '-a 1' is specified, the following files will also be output:

    space-orig_seg-antsct_dseg.nii.gz               - Segmentation in antsct format
    space-orig_seg-antsct_label-X_probseg.nii.gz    - Posteriors for class X (if -p 1 specified), using BIDS common label names

HELP

}

useGPU=1
outputPosteriors=0
outputAnts=0

while getopts "a:g:i:m:o:p:h" opt; do
  case $opt in
    a) outputAnts=$OPTARG;;
    g) useGPU=$OPTARG;;
    i) inputBIDS=$OPTARG;;
    m) maskBIDS=$OPTARG;;
    o) outputBIDS=$OPTARG;;
    p) outputPosteriors=$OPTARG;;
    h) help; exit 1;;
    \?) echo "Unknown option $OPTARG"; exit 2;;
    :) echo "Option $OPTARG requires an argument"; exit 2;;
  esac
done

shift $((OPTIND-1))

if [[ $# -lt 2 ]]; then
  echo "Error: subject and session are required as positional arguments"
  exit 1
fi

subject=$1
session=$2

# find T1w images in the input dataset, relative to the dataset directory
imageList=($(find "${inputBIDS}/sub-${subject}/ses-${session}" -type f -name "*_T1w.nii.gz" \
                -printf "sub-${subject}/ses-${session}/%P "))

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

antsArg=""

if [[ $outputAnts -gt 0 ]]; then
  antsArg="--antsct"
fi

# Makes python output unbuffered, so we can tail the log file and see progress
# and errors in order
export PYTHONUNBUFFERED=1

if [[ ! -d "${outputBIDS}/code/logs" ]]; then
  mkdir -p "${outputBIDS}/code/logs"
fi

bsub -cwd . $gpuBsubOpt -o "${outputBIDS}/code/logs/synthseg_${date}_%J.txt" \
    conda run -p /project/ftdc_pipeline/ftdc-picsl/miniconda/envs/ftdc-picsl-cp311 ${repoDir}/scripts/run_synthseg.py \
      --container ${repoDir}/containers/synthseg-mask-0.4.0.sif $gpuScriptOpt $posteriorsArg $antsArg \
      --input-dataset $inputBIDS \
      --mask-dataset $maskBIDS \
      --output-dataset $outputBIDS \
      --anatomical-images ${imageList[@]} \
