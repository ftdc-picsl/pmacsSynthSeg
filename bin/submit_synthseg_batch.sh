#!/bin/bash

module load miniconda/3-22.11 > /dev/null
module load singularity/3.8.3

scriptPath=$(readlink -f "$0")
scriptDir=$(dirname "${scriptPath}")
# Repo base dir under which we find bin/ and containers/
repoDir=${scriptDir%/bin}

inputBIDS=""
maskBIDS=""
outputBIDS=""

function usage() {
  echo "Usage:
  $0 [-h] [-a 0/1] [-g 1/0] [-p 0/1] -i input_dataset -m mask_dataset -o output_dataset image_list.txt
  "
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

function help() {
cat << HELP
  `usage`

  This is a wrapper script to submit images for processing. It assumes input from a BIDS dataset, which by default is

    $inputBIDS

  The image_list should be one per line, relative to the BIDS dataset, eg

  sub-123456/ses-19970829x0214/anat/sub-123456_ses-19970829x0214_T1w.nii.gz

  Brain masks should exist in a BIDS derivative dataset, which by default is

    $maskBIDS

  Mask files should be named as derivatives of the T1w.nii.gz files, eg if the T1w input image is

    sub-123456_ses-20160429x0000_acq-mprage_T1w.nii.gz

  then the brain mask should be

    sub-123456_ses-20160429x0000_acq-mprage_desc-brain_mask.nii.gz

  The brain mask is only used to center the cropped FOV of synthseg. The input image is then cropped
  around the mask and resampled to 1mm isotropic resolution. This is the "SynthSeg space", which should
  be anatomically aligned with the native space, but with a different bounding box and spacing.

  Required args:

    -i input dataset
        Input BIDS dataset (default = $inputBIDS).
    -m mask dataset
        BIDS dataset containing brain masks (default = $maskBIDS).
    -o output dataset
        Output BIDS dataset.

  Optional args:

    -a 0/1
        Output in antsct format (default = 0).
    -g 1/0
        Use the GPU (default = 1).
    -p 0/1
        Output posteriors (default = 0).

  Positional args:

    image1 image2 ... or image_list.txt

    List of images to process. If a file is provided, it should contain one image per line, and have the extension .txt.

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
    space-orig_seg-antsct_label-X_probseg.nii.gz    - Posteriors for class X (if -p 1 specified)

HELP

}

imageList=""
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

date=`date +%Y%m%d`

gpuBsubOpt=""
gpuScrptOpt=""

if [[ $useGPU -gt 0 ]]; then
  gpuBsubOpt='"-gpu "num=1:mode=exclusive_process:mps=no:gtile=1"'
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

mkdir -p ${outputBIDS}/code/logs

bsub -cwd . $gpuBsubOpt -o "${outputBIDS}/code/logs/synthseg_${date}_%J.txt" \
    conda run -p /project/ftdc_pipeline/ftdc-picsl/miniconda/envs/ftdc-picsl-cp311 ${repoDir}/scripts/run_synthseg.py \
      --container ${repoDir}/containers/synthseg-mask-0.4.0.sif $gpuScriptOpt $posteriorsArg $antsArg \
      --input-dataset $inputBIDS \
      --mask-dataset $maskBIDS \
      --output-dataset $outputBIDS \
      --anatomical-images "$@"
