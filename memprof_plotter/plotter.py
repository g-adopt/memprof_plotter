#!/usr/bin/env python3

import argparse
import github
import github.Workflow
import github.WorkflowRun
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
ORDER BY
    t ASC
"""


class Memprof_Run:
    def __init__(self, run: github.WorkflowRun.WorkflowRun):
        self.head_branch = run.head_branch
        self.run_number = run.run_number

    def add_artefact(self, artefact: zipfile.ZipFile):
        self.artefact = artefact

    def __str__(self) -> str:
        return f"{self.run_number} - {self.head_branch}"


def download_artefact(url: str) -> zipfile.ZipFile | None:
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
        return None
    zf = zipfile.ZipFile(BytesIO(req.content))
    if "tsp_db.sqlite3" in zf.namelist():
        return zf
    else:
        print("Artefact does not contain required TSP database")
        return None


def get_artefacts(
    nruns: int, workflow: github.Workflow.Workflow, artefact: str, filter: list[str]
) -> dict[int, Memprof_Run]:
    runs = {}
    for run in workflow.get_runs(status="success"):
        if filter:
            if run.head_branch not in filter:
                continue
        mpr = Memprof_Run(run)
        for gha in run.get_artifacts():
            if gha.name == artefact:
                artefact_data = download_artefact(gha.archive_download_url)
                if artefact_data:
                    mpr.add_artefact(artefact_data)
                    runs[run.run_number] = mpr
                break
        if len(runs) == nruns:
            break
    return runs


class Zip_to_sql_conn:
    def __init__(self, zip: zipfile.ZipFile):
        self.db = zip.read("tsp_db.sqlite3")
        self.conn = sqlite3.connect(":memory:")
        self.tmpfile = None
        if hasattr(self.conn, "deserialize"):
            self.conn.deserialize(self.db)
        else:
            self.tmpfile = tempfile.NamedTemporaryFile()
            self.tmpfile.write(self.db)
            self.conn = sqlite3.connect(self.tmpfile.name)

    def __enter__(self) -> sqlite3.Connection:
        return self.conn

    def __exit__(self, type, value, traceback):
        self.conn.close()
        if self.tmpfile:
            self.tmpfile.close()


def check_memory_anomaly(category: str, test_name: str, rss: dict[int, list[float]], times: dict[int, list[float]]):
    """Check for unusually high memory usage in the latest test

    For every test with a >60 second runtime, compare the maximum memory usage of the most recent
    test and issue a warning via github annotation if the memory usage is more than 20% above the
    average of the n previous runs.
    """
    if len(rss) == 1:
        return

    if any([max(tl or [0.0,]) <= 60.0 for tl in times.values()]):
        return

    max_mems = {i: max(rl) for i, rl in rss.items()}
    latest_run_no = max(max_mems.keys())

    avg_mem = sum([m for i, m in max_mems.items() if i != latest_run_no]) / (len(max_mems) - 1)
    if max_mems[latest_run_no] > 1.2 * avg_mem:
        print(
            f"::warning title=High Memory Usage::Latest run of {category}: {test_name} ({latest_run_no}) has memory usage over 20% higher than the average for this test {max_mems[latest_run_no]:.2f}GB > {avg_mem:.2f}GB"
        )


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
    parser.add_argument(
        "-f",
        "--filter",
        required=False,
        type=str,
        default="",
        help="Comma separated list of branch names to filter runs on",
    )

    ns = parser.parse_args(sys.argv[1:])

    ### Connect to github
    auth = github.Auth.Token(gh_token)
    gh = github.Github(auth=auth)
    repo = gh.get_repo(ns.repo)

    filter = ns.filter.split(",") if ns.filter else []

    runs = get_artefacts(ns.nruns, repo.get_workflow(ns.workflow), ns.artefact, filter)

    d_times = defaultdict(dict)
    d_rss = defaultdict(dict)
    d_cat = {}
    d_names = {}

    for runid, mpr in runs.items():
        with Zip_to_sql_conn(mpr.artefact) as conn:
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

    for k, v in d_rss.items():
        check_memory_anomaly(d_cat[k], k, v, d_times[k])
        os.makedirs(f"{ns.outdir}/{d_cat[k]}", exist_ok=True)
        fig, ax = plt.subplots()
        for runid in runs:
            if runid in v:
                ax.plot(d_times[k][runid], v[runid], label=f"Run {runs[runid]}")
        ax.set_xlabel("Time (seconds)")
        ax.set_ylabel("Memory usage (GB)")
        ax.set_ylim(ymin=0.0)
        ax.set_title(d_names[k])
        ax.legend()
        fig.savefig(f"{ns.outdir}/{d_cat[k]}/{re.sub('[ /]', '', k)}.png")
        plt.close(fig)


if __name__ == "__main__":
    main()
