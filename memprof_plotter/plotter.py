#!/usr/bin/env python3

import argparse
import github
import github.Workflow
import matplotlib.pyplot as plt
import os
import re
import requests
import sqlite3
import sys
import tempfile
import zipfile

from collections import defaultdict
from io import BytesIO

gh_token = os.environ.get("GH_TOKEN", "BAD_KEY")

get_all_cmds_query: str = "SELECT command,category FROM jobs"

get_mem_query: str = """
SELECT
    command,
    category,
    (time - mt) / 1000000.0 AS t,
    rss / 1048576.0
FROM
    memprof
    JOIN jobs ON memprof.jobid = jobs.id
    JOIN (
        SELECT
            jobid,
            Min(TIME) AS mt
        FROM
            memprof
        GROUP BY
            jobid
    ) AS mintime ON memprof.jobid = mintime.jobid
"""


def download_artefact(url: str) -> bytes | None:
    """
    PyGithub does not support retrieving artefacts into buffers, so we have to resort
    to requests
    """
    req = requests.get(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {gh_token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    if req.status_code != 200:
        print(f"Failed to download archive {url}")
        return
    zf = zipfile.ZipFile(BytesIO(req.content))
    if "tsp_db.sqlite3" in zf.namelist():
        return zf.read("tsp_db.sqlite3")
    else:
        print("Artefact does not contain required TSP database")
        return
    


def get_artefacts(nruns: int, workflow: github.Workflow.Workflow, artefact: str) -> dict[int, bytes]:
    irun = 0
    runs = {}
    for run in workflow.get_runs(status="success"):
        k = run.run_number
        for gha in run.get_artifacts():
            if gha.name == artefact:
                artefact_data = download_artefact(gha.archive_download_url)
                if artefact_data:
                    runs[k] = artefact_data
                    irun += 1
                break
        if irun == nruns:
            break
    return runs


def main():

    if gh_token == "BAD_KEY":
        raise KeyError("GH_TOKEN must be set in environment")

    parser = argparse.ArgumentParser(
        prog="memprof_plotter", description="Plot memprof data from g-adopt github actions artefacts"
    )
    parser.add_argument(
        "-o", "--outdir", required=False, type=str, default="memprof_plots", help="top directory for output plots"
    )
    parser.add_argument(
        "-n", "--nruns", required=False, type=int, default=5, help="Number of successful runs to gather"
    )
    parser.add_argument(
        "-r", "--repo", required=False, type=str, default="g-adopt/g-adopt", help="Repository to gather artefacts from"
    )
    parser.add_argument(
        "-w",
        "--workflow",
        required=False,
        type=str,
        default="test.yml",
        help="Name of workflow file containing memprof data",
    )
    parser.add_argument(
        "-a", "--artefact", required=False, type=str, default="run-log", help="Name of artefact containing memprof data"
    )

    ns = parser.parse_args(sys.argv[1:])

    ### Connect to github
    auth = github.Auth.Token(gh_token)
    gh = github.Github(auth=auth)
    repo = gh.get_repo(ns.repo)

    runs = get_artefacts(ns.nruns, repo.get_workflow(ns.workflow), ns.artefact)

    d_times = defaultdict(dict)
    d_rss = defaultdict(dict)
    d_cat = {}
    d_names = {}

    tmpfile = None

    for runid, run in runs.items():
        conn = sqlite3.connect(":memory:")
        if hasattr(conn, "deserialize"):
            conn.deserialize(run)
        else:
            tmpfile = tempfile.TemporaryFile()
            tmpfile.write(run)
            conn = sqlite3.connect(tmpfile.name)
        cur = conn.cursor()
        cur.execute(get_all_cmds_query)
        for cmd, cat in cur.fetchall():
            d_cat[f"{cat}_{cmd}"] = cat or "other"
            d_times[f"{cat}_{cmd}"][runid] = []
            d_rss[f"{cat}_{cmd}"][runid] = []
            d_names[f"{cat}_{cmd}"] = cmd

        try:
            cur.execute(get_mem_query)
        except sqlite3.OperationalError:
            ### No such table memprof
            continue
        for cmd, cat, time, rss in cur.fetchall():
            d_times[f"{cat}_{cmd}"][runid].append(time)
            d_rss[f"{cat}_{cmd}"][runid].append(rss)
        conn.close()
        if tmpfile:
            tmpfile.close()

    for k, v in d_rss.items():
        os.makedirs(f"{ns.outdir}/{d_cat[k]}", exist_ok=True)
        fig, ax = plt.subplots()
        for runid in runs:
            if runid in v:
                ax.plot(d_times[k][runid], v[runid], label=f"Run {runid}")
        ax.set_xlabel("Time (seconds)")
        ax.set_ylabel("Memory usage (GB)")
        ax.set_ylim(ymin=0.0)
        ax.set_title(d_names[k])
        ax.legend()
        fig.savefig(f"{ns.outdir}/{d_cat[k]}/{re.sub('[ /]', '', k)}.png")
        plt.close(fig)


if __name__ == "__main__":
    main()
