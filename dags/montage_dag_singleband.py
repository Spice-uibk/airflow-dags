from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
import json
from airflow.models import Variable


default_args = {
    "owner": "user",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SIZE = "0.25"

# MinIO Config
MINIO_ENDPOINT = "minio.stefan-dev.svc.cluster.local:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "montage-data"
REMOTE_TMP = "tmp_processing"
CONTEXT_FILE = "context_singleband.tar.gz"

NAMESPACE = "stefan-dev"
IMAGE = "kogsi/montage:latest"

minio_env_vars = [
    k8s.V1EnvVar(name="MINIO_ENDPOINT", value=MINIO_ENDPOINT),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value=MINIO_ACCESS_KEY),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY", value=MINIO_SECRET_KEY),
    k8s.V1EnvVar(name="PYTHONUNBUFFERED", value="1"),
]


def get_python_wrapper(mode="standard", cmd=None, subdirs=None,
                       split_cfg=None, merge_cfg=None,
                       shard_idx=None, final_output=None,
                       cleanup_targets=None):
    """
    Wrapper code remains identical to ensure compatibility with both
    parallel and sequential modes.
    """
    subdir_str = " ".join(subdirs) if subdirs else ""
    split_json = json.dumps(split_cfg) if split_cfg else "None"
    merge_json = json.dumps(merge_cfg) if merge_cfg else "None"
    cleanup_str = " ".join(cleanup_targets) if cleanup_targets else ""

    script = f"""
import subprocess, os, sys, glob, math, shutil, logging

# logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("{mode}")

BUCKET = "{MINIO_BUCKET}"
REMOTE_TMP = "{REMOTE_TMP}"
CONTEXT = "{CONTEXT_FILE}"
MODE = "{mode}"
CMD = {repr(cmd)}
SUBDIRS = "{subdir_str}"
SHARD_IDX = {shard_idx if shard_idx is not None else 'None'}
FINAL_OUTPUT = "{final_output or ''}"
SPLIT_CFG = {split_json}
MERGE_CFG = {merge_json}
CLEANUP_TARGETS = "{cleanup_str}".split()

def run(c, ignore=False):
    logger.info(f"Exec: {{c}}")
    try: 
        subprocess.check_call(c, shell=True)
    except subprocess.CalledProcessError as e:
        if not ignore: 
            logger.error(f"Command failed: {{e}}")
            raise e
        logger.warning(f"Ignored error: {{e}}")

def mc_cp(src, dst):
    run(f"mc cp {{src}} {{dst}}")

def main():
    logger.info(f"Starting Task | Mode: {{MODE}} | Shard: {{SHARD_IDX}}")

    run(f"mc alias set myminio http://$MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY")

    # download context only if they need headers/tables (Standard, Split, Merge)
    logger.info("Downloading Context...")
    run(f"mc cp myminio/{{BUCKET}}/{{REMOTE_TMP}}/{{CONTEXT}} /tmp/{{CONTEXT}}", ignore=True)
    if os.path.exists(f"/tmp/{{CONTEXT}}"):
        run(f"tar -xzf /tmp/{{CONTEXT}} -C . && rm /tmp/{{CONTEXT}}")
    else:
        logger.info("No context found (fresh start).")

    # download shards only worker/merger
    if MODE == "worker":
        s_in = f"shard_in_{{SHARD_IDX}}.tar.gz"
        logger.info(f"Downloading Input Shard: {{s_in}}")
        run(f"mc cp myminio/{{BUCKET}}/{{REMOTE_TMP}}/{{s_in}} /tmp/{{s_in}}")
        run(f"tar -xzf /tmp/{{s_in}} -C . && rm /tmp/{{s_in}}")

    if MODE == "merge":
        prefix = MERGE_CFG["shard_prefix"]
        for i in range(MERGE_CFG['n_parts']):
            s_out = f"{{prefix}}_{{i}}.tar.gz"
            logger.info(f"Downloading Output Shard: {{s_out}}")
            try:
                run(f"mc cp myminio/{{BUCKET}}/{{REMOTE_TMP}}/{{s_out}} /tmp/{{s_out}}")
                run(f"tar -xzf /tmp/{{s_out}} -C . && rm /tmp/{{s_out}}")
            except:
                logger.warning(f"Missing shard {{i}} (file: {{s_out}})")

    # prepare directories
    if SUBDIRS: 
        logger.info(f"Creating directories: {{SUBDIRS}}")
        run(f"mkdir -p {{SUBDIRS}}")

    # execute logic
    if MODE == "split":
        tbl = SPLIT_CFG["input_tbl"]
        base = SPLIT_CFG["output_base"]
        n = SPLIT_CFG["n_parts"]
        logger.info(f"Splitting {{tbl}} into {{n}} parts...")

        with open(tbl, 'r') as f: lines = f.readlines()
        header = [l for l in lines if l.startswith('|') or l.startswith('\\\\')]
        data = [l for l in lines if not (l.startswith('|') or l.startswith('\\\\'))]

        chunk_size = math.ceil(len(data) / n)
        if chunk_size == 0: chunk_size = 1

        for i in range(n):
            chunk = data[i*chunk_size : (i+1)*chunk_size]
            fname = f"{{base}}_{{i}}.tbl"
            with open(fname, 'w') as f_out: f_out.writelines(header + chunk)

            tar_name = f"shard_in_{{i}}.tar.gz"
            logger.info(f"Packing Shard {{i}}: {{tar_name}}")
            run(f"tar -czf /tmp/{{tar_name}} {{fname}}")
            mc_cp(f"/tmp/{{tar_name}}", f"myminio/{{BUCKET}}/{{REMOTE_TMP}}/{{tar_name}}")

    elif MODE == "merge":
        if "tables_to_merge" in MERGE_CFG:
            for out_tbl in MERGE_CFG['tables_to_merge']:
                logger.info(f"Merging shards into {{out_tbl}}...")
                with open(out_tbl, 'w') as outfile:
                    first = True
                    for i in range(MERGE_CFG['n_parts']):
                        part_file = out_tbl.replace('.tbl', f'_{{i}}.tbl')
                        if os.path.exists(part_file):
                            with open(part_file, 'r') as infile:
                                for line in infile:
                                    if line.startswith('|') or line.startswith('\\\\'):
                                        if first: outfile.write(line)
                                    else:
                                        outfile.write(line)
                            os.remove(part_file) # Clean up shard table
                            first = False

    else:
        # Standard or Worker execution
        if CMD: 
            logger.info(f"Running Command: {{CMD}}")
            run(CMD)

    # pack and upload
    if MODE == 'worker':
        out_tar = f"{{MERGE_CFG['shard_prefix']}}_{{SHARD_IDX}}.tar.gz"
        logger.info(f"Worker finished. Uploading delta: {{out_tar}}")
        run(f"tar -czf /tmp/{{out_tar}} {{SUBDIRS}} *.tbl")
        mc_cp(f"/tmp/{{out_tar}}", f"myminio/{{BUCKET}}/{{REMOTE_TMP}}/{{out_tar}}")

    else:
        # standard or merge (both update global context)
        if CLEANUP_TARGETS:
            for target in CLEANUP_TARGETS:
                if target and os.path.exists(target):
                    logger.info(f"--- CLEANUP: Removing {{target}} to save space ---")
                    run(f"rm -rf {{target}}")

        logger.info("Uploading global context...")
        run(f"tar -czf /tmp/{{CONTEXT}} .")
        mc_cp(f"/tmp/{{CONTEXT}}", f"myminio/{{BUCKET}}/{{REMOTE_TMP}}/{{CONTEXT}}")

    # final export
    if FINAL_OUTPUT and os.path.exists(FINAL_OUTPUT):
        logger.info(f"Exporting Final Output: {{FINAL_OUTPUT}}")
        mc_cp(FINAL_OUTPUT, f"myminio/{{BUCKET}}/{{FINAL_OUTPUT}}")

    logger.info("--- Task Completed Successfully ---")

if __name__ == "__main__":
    if not os.path.exists("/work"): os.makedirs("/work")
    os.chdir("/work")
    try: main()
    except Exception as e:
        logging.critical(f"Critical Failure: {{e}}")
        sys.exit(1)
"""
    return script


with DAG(
        dag_id="montage_data_singleband",
        default_args=default_args,
        schedule=None,
        catchup=False,
) as dag:

    PARALLEL_DOWNLOAD = int(Variable.get("montage_singleband_parallel_download", default_var=1))
    PARALLEL_PROJ = int(Variable.get("montage_singleband_parallel_projection", default_var=1))
    PARALLEL_DIFF = int(Variable.get("montage_singleband_parallel_diff", default_var=1))

    def task_builder(task_id, mode="standard", cmd=None, subdirs=None,
                     split_cfg=None, merge_cfg=None, shard_idx=None, final_output=None,
                     cleanup_targets=None):
        return KubernetesPodOperator(
            task_id=task_id,
            name=f"montage-{task_id.lower().replace('_', '-')}",
            namespace=NAMESPACE,
            image=IMAGE,
            image_pull_policy="IfNotPresent",
            env_vars=minio_env_vars,
            cmds=["python3", "-c"],
            arguments=[get_python_wrapper(mode, cmd, subdirs, split_cfg, merge_cfg,
                                          shard_idx, final_output, cleanup_targets)],
            get_logs=True,
            volumes=[k8s.V1Volume(name="scratch", empty_dir=k8s.V1EmptyDirVolumeSource())],
            volume_mounts=[k8s.V1VolumeMount(name="scratch", mount_path="/work")],
            node_selector={"kubernetes.io/hostname": "node1"},
        )


    # clean start
    cleanup = KubernetesPodOperator(
        task_id="Cleanup",
        name="cleanup",
        namespace=NAMESPACE,
        image=IMAGE,
        env_vars=minio_env_vars,
        cmds=["bash", "-c"],
        arguments=[
            f"mc alias set myminio http://$MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY && "
            f"mc rm --recursive --force myminio/{MINIO_BUCKET}/{REMOTE_TMP}/ || true"
        ],
        get_logs=True,
        node_selector={"kubernetes.io/hostname": "node1"},
    )

    m_hdr = task_builder("mHdr", cmd=f"mHdr 'NGC 3372' {SIZE} region.hdr")
    m_archive = task_builder("mArchiveList", cmd="mArchiveList 2mass k 'NGC 3372' 2.8 2.8 archive.tbl")
    m_cov = task_builder("mCoverage", cmd="mCoverageCheck archive.tbl remote.tbl -header region.hdr")

    cleanup >> m_hdr >> m_archive >> m_cov

    current_head = m_cov

    # download phase
    if PARALLEL_DOWNLOAD > 1:
        split_dl = task_builder("Split_DL", mode="split",
                                split_cfg={'input_tbl': 'remote.tbl', 'output_base': 'remote_shard',
                                           'n_parts': PARALLEL_DOWNLOAD})

        dl_workers = []
        for i in range(PARALLEL_DOWNLOAD):
            w = task_builder(f"DL_Worker_{i}", mode="worker", shard_idx=i, subdirs=["raw"],
                             merge_cfg={'shard_prefix': 'raw_out'},
                             cmd=f"mArchiveExec -p raw remote_shard_{i}.tbl")
            dl_workers.append(w)

        merge_dl = task_builder("Merge_DL", mode="merge",
                                merge_cfg={'shard_prefix': 'raw_out', 'n_parts': PARALLEL_DOWNLOAD},
                                cleanup_targets=[])

        current_head >> split_dl
        for w in dl_workers:
            split_dl >> w >> merge_dl

        current_head = merge_dl

    else:  # sequential execution
        dl_single = task_builder("DL_Sequential", mode="standard", subdirs=["raw"],
                                 cmd="mArchiveExec -p raw remote.tbl")

        current_head >> dl_single
        current_head = dl_single

    m_img_raw = task_builder("mImgtbl_Raw", cmd="mImgtbl raw rimages.tbl")
    current_head >> m_img_raw
    current_head = m_img_raw

    # projection phase
    if PARALLEL_PROJ > 1:
        split_proj = task_builder("Split_Proj", mode="split",
                                  split_cfg={'input_tbl': 'rimages.tbl', 'output_base': 'rimages_shard',
                                             'n_parts': PARALLEL_PROJ})

        proj_workers = []
        for i in range(PARALLEL_PROJ):
            w = task_builder(f"Proj_Worker_{i}", mode="worker", shard_idx=i, subdirs=["projected"],
                             merge_cfg={'shard_prefix': 'proj_out'},
                             cmd=f"mProjExec -q -p raw rimages_shard_{i}.tbl region.hdr projected stats_{i}.tbl")
            proj_workers.append(w)

        merge_proj = task_builder("Merge_Proj", mode="merge",
                                  merge_cfg={'shard_prefix': 'proj_out', 'n_parts': PARALLEL_PROJ,
                                             'tables_to_merge': ['stats.tbl']},
                                  cleanup_targets=["raw"])  # Clean up raw here to save space

        current_head >> split_proj
        for w in proj_workers:
            split_proj >> w >> merge_proj

        current_head = merge_proj

    else:  # sequential execution
        proj_single = task_builder("Proj_Sequential", mode="standard", subdirs=["projected"],
                                   cmd="mProjExec -q -p raw rimages.tbl region.hdr projected stats.tbl",
                                   cleanup_targets=["raw"])

        current_head >> proj_single
        current_head = proj_single

    m_img_proj = task_builder("mImgtbl_Proj", cmd="mImgtbl projected pimages.tbl")
    m_overlaps = task_builder("mOverlaps", cmd="mOverlaps pimages.tbl diffs.tbl", subdirs=["diffs"])

    current_head >> m_img_proj >> m_overlaps
    current_head = m_overlaps

    # diff/fit phase
    if PARALLEL_DIFF > 1:
        split_diff = task_builder("Split_Diff", mode="split",
                                  split_cfg={'input_tbl': 'diffs.tbl', 'output_base': 'diffs_shard',
                                             'n_parts': PARALLEL_DIFF})

        diff_workers = []
        for i in range(PARALLEL_DIFF):
            w = task_builder(f"Diff_Worker_{i}", mode="worker", shard_idx=i, subdirs=["diffs"],
                             merge_cfg={'shard_prefix': 'diff_out'},
                             cmd=f"mDiffFitExec -p projected diffs_shard_{i}.tbl region.hdr diffs fits_{i}.tbl")
            diff_workers.append(w)

        merge_diff = task_builder("Merge_Diff", mode="merge",
                                  merge_cfg={'shard_prefix': 'diff_out', 'n_parts': PARALLEL_DIFF,
                                             'tables_to_merge': ['fits.tbl']},
                                  cleanup_targets=["diffs"])

        current_head >> split_diff
        for w in diff_workers:
            split_diff >> w >> merge_diff

        current_head = merge_diff

    else:  # sequential execution
        diff_single = task_builder("Diff_Sequential", mode="standard", subdirs=["diffs"],
                                   cmd="mDiffFitExec -p projected diffs.tbl region.hdr diffs fits.tbl",
                                   cleanup_targets=["diffs"])

        current_head >> diff_single
        current_head = diff_single

    m_bg_model = task_builder("mBgModel", cmd="mBgModel pimages.tbl fits.tbl corrections.tbl")

    m_bg_exec = task_builder("mBgExec", cmd="mBgExec -p projected pimages.tbl corrections.tbl corrected",
                             subdirs=["corrected"],
                             cleanup_targets=["projected"])

    m_img_corr = task_builder("mImgtbl_Corr", cmd="mImgtbl corrected cimages.tbl")

    m_add = task_builder("mAdd", cmd="mAdd -p corrected cimages.tbl region.hdr mosaic.fits")

    m_view = task_builder("mViewer",
                          cmd="mViewer -ct 1 -gray mosaic.fits -2s max gaussian-log -out mosaic_singleband.png",
                          final_output="mosaic_singleband.png")

    current_head >> m_bg_model >> m_bg_exec >> m_img_corr >> m_add >> m_view