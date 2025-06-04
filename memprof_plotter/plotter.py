#!/usr/bin/env python3

import re
import os
import sqlite3
import sys
import matplotlib.pyplot as plt

from collections import defaultdict

get_all_cmds_query = "SELECT command,category FROM jobs"

get_mem_query = """
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


def main():
    d_times = defaultdict(dict)
    d_rss = defaultdict(dict)
    d_cat = {}
    d_names = {}

    for runid in sys.argv[1:]:
        conn = sqlite3.connect(f"{runid}/tsp_db.sqlite3")
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

    for k, v in d_rss.items():
        os.makedirs(f"plots/{d_cat[k]}", exist_ok=True)
        fig, ax = plt.subplots()
        for runid in sys.argv[1:]:
            if runid in v:
                ax.plot(d_times[k][runid], v[runid], label=f"Run {runid}")
        ax.set_xlabel("Time (seconds)")
        ax.set_ylabel("Memory usage (GB)")
        ax.set_ylim(ymin=0.0)
        ax.set_title(d_names[k])
        ax.legend()
        fig.savefig(f"plots/{d_cat[k]}/{re.sub('[ /]', '', k)}.png")
        plt.close(fig)


if __name__ == "__main__":
    main()
