#!/usr/bin/env python

import argparse
import filelock
import glob
import json
import os
import re
import shutil
import subprocess
import tempfile


def get_dataset_name(dataset_path):
    """Get a dataset name from the BIDS dataset_description.json

    Parameters:
    -----------
    dataset_path : str
        Path to the dataset directory.

    Returns:
    --------
    str :
        The dataset name.
    """
    description_file = os.path.join(dataset_path, 'dataset_description.json')
    if not os.path.exists(description_file):
        raise FileNotFoundError(f"dataset_description.json not found in dataset path {dataset_path}")

    with open(description_file, 'r') as f:
        ds_description = json.load(f)

    if 'Name' not in ds_description:
        raise ValueError("Dataset name ('Name') not found in dataset_description.json")

    return ds_description['Name']


def get_container_info(container):

    container_info = subprocess.run(['singularity', 'inspect', container], stdout=subprocess.PIPE)

    # Parse the container info to get the tag
    container_info = container_info.stdout.decode('utf-8')

    container_git_remote = None
    container_tag = None
    container_version = None

    for line in container_info.split('\n'):
        if 'org.label-schema.usage.singularity.deffile.from' in line:
            # example line: org.label-schema.usage.singularity.deffile.from: cookpa/synthseg-mask:0.4.1
            container_version = line.split(':')[-1].strip()
            container_tag = line.split(':')[-2].strip() + ':' + container_version
        if 'git.remote:' in line:
            container_git_remote = line.split(' ')[-1].strip()

    if container_tag is None:
        raise ValueError(f"Container tag not found in {container}")

    return {'tag': container_tag, 'version': container_version, 'git_remote': container_git_remote}


# Get a dictionary for the GeneratedBy field for the BIDS dataset_description.json
# This is used to record the software used to generate the dataset
# The environment variables DOCKER_IMAGE_TAG and DOCKER_IMAGE_VERSION are used if set
#
# Container type is assumed to be "docker" unless the variable SINGULARITY_CONTAINER
# is defined
def get_generated_by(container_info, existing_generated_by=None):

    import copy

    generated_by = []

    container_type = 'singularity'

    if existing_generated_by is not None:
        generated_by = copy.deepcopy(existing_generated_by)
        for gb in existing_generated_by:
            if gb['Name'] == 'SynthSeg' and gb['Container']['Tag'] == container_info['tag']:
                # Don't overwrite existing generated_by if it's already set to this pipeline
                return generated_by

    container_type = 'singularity'

    if 'SINGULARITY_CONTAINER' in os.environ:
        container_type = 'singularity'

    gen_dict = {'Name': 'SynthSeg',
                'Version': container_info['version'],
                'CodeURL': container_info['git_remote'],
                'Container': {'Type': container_type, 'Tag': container_info['tag']}
                }

    generated_by.append(gen_dict)
    return generated_by


def update_output_dataset(output_dataset_dir, input_dataset_dir, container_info):

    input_dataset_name = get_dataset_name(input_dataset_dir)

    lock_file = os.path.join(output_dataset_dir, 'synthseg_dataset_metadata.lock')

    if os.path.exists(lock_file):
        print(f"WARNING: lock file exists in dataset {output_dataset_dir}. Will wait for it to be released.")

    with filelock.SoftFileLock(lock_file, timeout=30):
        if not os.path.exists(os.path.join(output_dataset_dir, 'dataset_description.json')):
            # Write dataset_description.json
            output_dataset_name = input_dataset_name + '_synthseg'

            output_ds_description = {'Name': output_dataset_name, 'BIDSVersion': '1.8.0',
                                    'DatasetType': 'derivative', 'GeneratedBy': get_generated_by(container_info)
                                    }
            # Write json to output dataset
            with open(os.path.join(output_dataset_dir, 'dataset_description.json'), 'w') as file_out:
                json.dump(output_ds_description, file_out, indent=2, sort_keys=True)
        else:
            # Get output dataset metadata
            try:
                with open(f"{output_dataset_dir}/dataset_description.json", 'r') as file_in:
                    output_dataset_json = json.load(file_in)
                # If this container doesn't already exist in the generated_by list, it will be added
                if 'GeneratedBy' in output_dataset_json:
                    generated_by = get_generated_by(container_info, output_dataset_json['GeneratedBy'])
                else:
                    generated_by = get_generated_by(container_info)
                # If we updated the generated_by, write it back to the output dataset
                output_dataset_name = output_dataset_json['Name']
                old_gen_by = output_dataset_json['GeneratedBy']
                if old_gen_by is None or len(generated_by) > len(old_gen_by):
                    output_dataset_json['GeneratedBy'] = generated_by
                    with open(f"{output_dataset_dir}/dataset_description.json", 'w') as file_out:
                        json.dump(output_dataset_json, file_out, indent=2, sort_keys=True)
            except (FileNotFoundError, KeyError):
                raise ValueError(f"Output dataset Name is required, please check "
                                f"{output_dataset_dir}/data_description.json")


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
required.add_argument("--anatomical-images", help="List of anatomical images relative to the input data set. Either a list of "
                      "strings or a single text file. Multiple images from the same session must be distinguished before the "
                      "suffix. For example, 'acq-mprage_T1w.nii.gz' and 'acq-vnav_T1w.nii.gz' will be processed, but "
                      "'acq-vnav_T1w.nii.gz' and 'acq-vnav_T2w.nii.gz' from within the same session will not work.",
                      type=str, nargs='+', required=True)
optional = parser.add_argument_group('Optional arguments')
optional.add_argument("-h", "--help", action="help", help="show this help message and exit")
optional.add_argument("--gpu", help="Use GPU", action="store_true")
optional.add_argument("--posteriors", help="Output posteriors", action="store_true")
optional.add_argument("--antsct", help="Output antsct seg and posteriors", action="store_true")
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

anatomical_images = args.anatomical_images

if len(anatomical_images) == 1 and anatomical_images[0].endswith('.txt'):
    with open(args.anatomical_images[0], 'r') as file_in:
        anatomical_images = file_in.readlines()
    anatomical_images = [ line.rstrip() for line in anatomical_images ]

print(f"\nProcessing {len(anatomical_images)} images")

# Check if output bids dir exists, and if not, create it
if not os.path.isdir(output_dataset_dir):
    os.makedirs(output_dataset_dir, exist_ok = True)

container = args.container

container_info = get_container_info(container)

update_output_dataset(output_dataset_dir, input_dataset_dir, container_info)

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

    # find brain masks matching the anatomical prefix
    brain_mask_full_path_prefix = os.path.join(mask_dataset_dir, anatomical_prefix)

    # This is a backwards compatibilty hack because the brain masking routines behave differently
    # some scripts used -space-{modality}, eg space-T1w, and other don't use space- at all. This isn't good
    # because space- is supposed to denote a transformed version of a single image, not multiple images derived
    # independently.
    #
    brain_mask_files = glob.glob(f"{brain_mask_full_path_prefix}*_desc-brain_mask.nii.gz")

    if len(brain_mask_files) == 0:
        print(f"ERROR: Brain mask not found under: {brain_mask_full_path_prefix}")
        continue
    if len(brain_mask_files) > 1:
        print(f"ERROR: Multiple brain masks found for {input_anatomical}: {brain_mask_files}")
        continue

    # Brain mask relative to mask dataset
    brain_mask = os.path.relpath(brain_mask_files[0], mask_dataset_dir)

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

    if args.antsct:
        synthseg_cmd_list.append('--antsct')

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
    # 24        CSF
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
    dseg_label_dict = {0:"background", 2:"left_cerebral_white_matter", 3:"left_cerebral_cortex",
                       4:"left_lateral_ventricle", 5:"left_inferior_lateral_ventricle", 7:"left_cerebellum_white_matter",
                       8:"left_cerebellum_cortex", 10:"left_thalamus", 11:"left_caudate", 12:"left_putamen",
                       13:"left_pallidum", 14:"3rd_ventricle", 15:"4th_ventricle", 16:"brain-stem", 17:"left_hippocampus",
                       18:"left_amygdala", 24:"CSF", 26:"left_accumbens_area", 28:"left_ventral_DC",
                       41:"right_cerebral_white_matter", 42:"right_cerebral_cortex", 43:"right_lateral_ventricle",
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


    if (args.antsct):
        ants_seg_out_prefix = f"{anatomical_prefix}_space-orig_seg-antsct_dseg"

        shutil.copy(f"{working_dir}/{anatomical_prefix}SynthSegToAntsCT.nii.gz",
                f"{output_dataset_dir}/{ants_seg_out_prefix}.nii.gz")

        with open(f"{output_dataset_dir}/{ants_seg_out_prefix}.tsv", "w") as ants_dseg_tsv:
            ants_dseg_tsv.write("index\tname\n")
            ants_dseg_tsv.write(f"0\tBackground\n")
            ants_dseg_tsv.write(f"1\tCSF\n")
            ants_dseg_tsv.write(f"2\tGray Matter\n")
            ants_dseg_tsv.write(f"3\tWhite Matter\n")
            ants_dseg_tsv.write(f"4\tSubcortical Gray Matter\n")
            ants_dseg_tsv.write(f"5\tBrainstem\n")
            ants_dseg_tsv.write(f"6\tCerebellum\n")

        if args.posteriors:
            ants_post_out_prefix = f"{anatomical_prefix}_space-orig_seg-antsct"
            bids_posterior_labels = ['CSF', 'CGM', 'WM', 'SGM', 'BS', 'CBM']
            for idx in range(6):
                shutil.copy(f"{working_dir}/{anatomical_prefix}AntsctPosteriors{idx+1}.nii.gz",
                            f"{output_dataset_dir}/{ants_post_out_prefix}_label-{bids_posterior_labels[idx]}_probseg.nii.gz")
