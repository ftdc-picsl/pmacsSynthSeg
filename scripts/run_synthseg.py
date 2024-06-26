#!/usr/bin/env python

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import tempfile


def csv_to_bids_tsv(input_file, output_file):
    # Read input_file
    with open(input_file, 'r') as file_in:
        header = file_in.readline()
        # replace spaces in header with underscores
        header = header.rstrip()
        header = header.replace(' ', '_')
        header = header.replace(',', '\t')
        # read remaining lines, translating commas to tabs
        lines = file_in.readlines()
        lines = [ line.rstrip() for line in lines ]
        lines = [ line.replace(',', '\t') for line in lines ]
    # open output file
    with open(output_file, 'w') as file_out:
        # write header
        file_out.write(header)
        file_out.write('\n')
        # write lines
        for line in lines:
            file_out.write(line)
            file_out.write('\n')



parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                 prog="synthseg brain segmentation", add_help = False, description='''

Batch interface for segmentation with synthseg.

Requires:
  singularity

''')
required = parser.add_argument_group('Required arguments')
required.add_argument("--container", help="Path to the container to run", type=str, required=True)
required.add_argument("--input-dataset", help="Input BIDS dataset dir, containing the source images", type=str, required=True)
required.add_argument("--mask-dataset", help="Mask BIDS dataset dir, containing the brain mask images", type=str, required=True)
required.add_argument("--output-dataset", help="Output BIDS dataset dir", type=str, required=True)
required.add_argument("--anatomical-images", help="List of anatomical images relative to the input data set. Multiple images from the same session must be distinguished "
                      "before the suffix. For example, 'acq-mprage_T1w.nii.gz' and 'acq-vnav_T1w.nii.gz' will be processed, but 'acq-vnav_T1w.nii.gz' and 'acq-vnav_T2w.nii.gz' "
                      "from within the same session will not work.", type=str, required=True)
optional = parser.add_argument_group('Optional arguments')
optional.add_argument("-h", "--help", action="help", help="show this help message and exit")
optional.add_argument("--gpu", help="Use GPU", action="store_true")
optional.add_argument("--posteriors", help="Output posteriors", action="store_true")
args = parser.parse_args()

script_dir = os.path.dirname(os.path.realpath(__file__))

input_dataset_dir = args.input_dataset
mask_dataset_dir = args.mask_dataset
output_dataset_dir = args.output_dataset

job_id = os.environ['LSB_JOBID']

# This is an object you can call cleanup() on manually
working_dir_tmpdir = tempfile.TemporaryDirectory(suffix=f".synthseg.{job_id}", dir="/scratch", ignore_cleanup_errors=True)
working_dir = working_dir_tmpdir.name

singularity_env = os.environ.copy()
singularity_env['SINGULARITYENV_OMP_NUM_THREADS'] = "1"
singularity_env['SINGULARITYENV_ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS'] = "1"
singularity_env['SINGULARITYENV_PYTHONUNBUFFERED'] = "1"

if shutil.which('singularity') is None:
    raise RuntimeError('singularity executable not found')

with open(args.anatomical_images, 'r') as file_in:
    anatomical_images = file_in.readlines()

anatomical_images = [ line.rstrip() for line in anatomical_images ]

print(f"\nProcessing {len(anatomical_images)} images")

# Check if output bids dir exists, and if not, create it
if not os.path.isdir(output_dataset_dir):
    os.makedirs(output_dataset_dir)

container = args.container

# Now process input data
for input_anatomical in anatomical_images:

    input_anatomical_full_path = os.path.join(input_dataset_dir, input_anatomical)

    if not os.path.isfile(input_anatomical_full_path):
        print(f"Anatomical input file not found: {input_anatomical_full_path}")
        continue

    print(f"\nProcessing {input_anatomical}")

    match = re.match('(.*)_(\w+)\.nii\.gz$', input_anatomical)

    # path and file relative to input dataset
    anatomical_prefix = match.group(1)

    # suffix eg T1w, T2w
    anatomical_suffix = match.group(2)

    # Mask path relative to mask data set
    # This should be good enough to ensure we don't mismatch mask and data
    brain_mask = f"{anatomical_prefix}_space-{anatomical_suffix}_desc-brain_mask.nii.gz"

    brain_mask_full_path = os.path.join(mask_dataset_dir, brain_mask)

    if not os.path.isfile(brain_mask_full_path):
        print(f"Brain mask not found: {brain_mask_full_path}")
        continue

    # Output dir for this session
    output_dir = os.path.realpath(os.path.dirname(os.path.join(output_dataset_dir, anatomical_prefix)))

    # Make output dir if needed
    os.makedirs(output_dir, exist_ok = True)

    # Segmentation output prefix relative to output dataset dir, in synthseg space (cropped, 1mm)
    seg_out_prefix = f"{anatomical_prefix}_space-SynthSeg_dseg"

    # Check for existing output for this particular T1w
    seg_out_full_path = os.path.join(output_dataset_dir, f"{seg_out_prefix}.nii.gz")
    if os.path.exists(seg_out_full_path):
        print(f"Output already exists: {seg_out_full_path}")
        continue

    # Now call synthseg - output to working_dir, will be renamed
    synthseg_cmd_list = ['singularity', 'run', '--cleanenv']

    if args.gpu:
        synthseg_cmd_list.append('--nv')

    synthseg_cmd_list.extend(['-B',
                f"{os.path.realpath(input_dataset_dir)}:/input,{os.path.realpath(mask_dataset_dir)}:/masks," +
                f"{os.path.realpath(working_dir)}:/output",
                container, '--input', f"/input/{input_anatomical}", "--mask", f"/masks/{brain_mask}",
                "--output", f"/output/{anatomical_prefix}", "--qc", "--vol", "--resample-orig"])

    # subprocess.run does not like empty args, instead append if needed
    if args.posteriors:
        synthseg_cmd_list.append('--post')

    print("---SynthSeg call---\n" + " ".join(synthseg_cmd_list) + "\n---")

    subprocess.run(synthseg_cmd_list, env=singularity_env)

    # Rename output in BIDS derivatives format
    shutil.copy(f"{working_dir}/{anatomical_prefix}SynthSeg.nii.gz", seg_out_full_path)

    # if run without cortical parcellation, the labels are specified in
    # https://github.com/BBillot/SynthSeg/blob/master/data/labels%20table.txt
    # labels    structures
    # 0         background
    # 2         left cerebral white matter
    # 3         left cerebral cortex
    # 4         left lateral ventricle
    # 5         left inferior lateral ventricle
    # 7         left cerebellum white matter
    # 8         left cerebellum cortex
    # 10        left thalamus
    # 11        left caudate
    # 12        left putamen
    # 13        left pallidum
    # 14        3rd ventricle
    # 15        4th ventricle
    # 16        brain-stem
    # 17        left hippocampus
    # 18        left amygdala
    # 26        left accumbens area
    # 28        left ventral DC
    # 41        right cerebral white matter
    # 42        right cerebral cortex
    # 43        right lateral ventricle
    # 44        right inferior lateral ventricle
    # 46        right cerebellum white matter
    # 47        right cerebellum cortex
    # 49        right thalamus
    # 50        right caudate
    # 51        right putamen
    # 52        right pallidum
    # 53        right hippocampus
    # 54        right amygdala
    # 58        right accumbens area
    # 60        right ventral DC
    dseg_label_dict = {0:"background", 2:"left_cerebral_white_matter", 3:"left_cerebral_cortex", 4:"left_lateral_ventricle",
                       5:"left_inferior_lateral_ventricle", 7:"left_cerebellum_white_matter", 8:"left_cerebellum_cortex",
                       10:"left_thalamus", 11:"left_caudate", 12:"left_putamen", 13:"left_pallidum", 14:"3rd_ventricle",
                       15:"4th_ventricle", 16:"brain-stem", 17:"left_hippocampus", 18:"left_amygdala", 26:"left_accumbens_area",
                       28:"left_ventral_DC", 41:"right_cerebral_white_matter", 42:"right_cerebral_cortex", 43:"right_lateral_ventricle",
                       44:"right_inferior_lateral_ventricle", 46:"right_cerebellum_white_matter", 47:"right_cerebellum_cortex",
                       49:"right_thalamus", 50:"right_caudate", 51:"right_putamen", 52:"right_pallidum", 53:"right_hippocampus",
                       54:"right_amygdala", 58:"right_accumbens_area", 60:"right_ventral_DC"}

    # probseg needs list of label names only, without indices
    # Need names in the same order as the indices
    dseg_label_keys = sorted(list(dseg_label_dict.keys()))
    dseg_label_names = [dseg_label_dict[key] for key in dseg_label_keys]

    # Write out dseg.tsv with column headers "index", "name"
    with open(f"{output_dataset_dir}/{seg_out_prefix}.tsv", "w") as dseg_tsv:
        dseg_tsv.write("index\tname\n")
        for index, name in dseg_label_dict.items():
            dseg_tsv.write(f"{index}\t{name}\n")

    # Should be a json with spatial reference (ie, the SynthSeg T1w)

    shutil.copy(f"{working_dir}/{anatomical_prefix}SynthSegInput.nii.gz",
                f"{output_dataset_dir}/{anatomical_prefix}_space-SynthSeg_{anatomical_suffix}.nii.gz")

    seg_out_orig_prefix = f"{anatomical_prefix}_space-orig_dseg"

    shutil.copy(f"{working_dir}/{anatomical_prefix}SynthSegOrig.nii.gz",
                f"{output_dataset_dir}/{seg_out_orig_prefix}.nii.gz")

    # labels the same for both images
    shutil.copy(f"{output_dataset_dir}/{seg_out_prefix}.tsv", f"{output_dataset_dir}/{seg_out_orig_prefix}.tsv")

    # Map QC and volumes CSV to TSV for BIDS
    csv_to_bids_tsv(f"{working_dir}/{anatomical_prefix}QC.csv",
                    f"{output_dataset_dir}/{anatomical_prefix}_desc-qc.tsv")
    csv_to_bids_tsv(f"{working_dir}/{anatomical_prefix}Volumes.csv",
                    f"{output_dataset_dir}/{anatomical_prefix}_desc-volumes.tsv")

    if (args.posteriors):

        posterior_out_prefix = f"{anatomical_prefix}_space-SynthSeg_probseg"

        shutil.copy(f"{working_dir}/{anatomical_prefix}Posteriors.nii.gz",
                f"{output_dataset_dir}/{posterior_out_prefix}.nii.gz")

        # Convert label map to JSON representation
        label_map = { "LabelMap" : dseg_label_names }
        with open(f"{output_dataset_dir}/{posterior_out_prefix}.json", "w") as post_json_file:
            json.dump(label_map, post_json_file, indent=4)

        posterior_out_orig_prefix = f"{anatomical_prefix}_space-orig_probseg"

        shutil.copy(f"{working_dir}/{anatomical_prefix}PosteriorsOrig.nii.gz",
                f"{output_dataset_dir}/{posterior_out_orig_prefix}.nii.gz")

        shutil.copy(f"{output_dataset_dir}/{posterior_out_prefix}.json", f"{output_dataset_dir}/{posterior_out_orig_prefix}.json")

