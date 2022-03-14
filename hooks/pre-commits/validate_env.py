#!/usr/bin/env python3

# Copyright (c) Imply Data, Inc. All rights reserved.
#
# This software is the confidential and proprietary information
# of Imply Data, Inc. You shall not disclose such Confidential
# Information and shall use it only in accordance with the terms
# of the license agreement you entered into with Imply.

import subprocess


SAFE_REMOTES=[
    'git@github.com:implydata/druid.git',
    'https://github.com/implydata/druid.git',
    'no-pushing'
]


all_remotes = subprocess.check_output("git remote", shell=True).decode('UTF-8')
for remote in all_remotes.splitlines():
    push_url = subprocess.check_output(['git','remote','get-url','--push',remote]).decode('UTF-8').strip()
    if push_url not in SAFE_REMOTES:
        raise Exception("BUSTED! \"{}\" is a non-safe pushable remote repository ({}). Please read 'https://implydata.atlassian.net/wiki/spaces/ENG/pages/997621788/Druid+Development+Getting+Started' and update your git remote repository setting.".format(remote, push_url))
