#!/usr/bin/env python3

# Copyright (c) Imply Data, Inc. All rights reserved.
#
# This software is the confidential and proprietary information
# of Imply Data, Inc. You shall not disclose such Confidential
# Information and shall use it only in accordance with the terms
# of the license agreement you entered into with Imply.

import sys


SAFE_REMOTES=[
    'git@github.com:implydata/druid.git',
    'https://github.com/implydata/druid.git'
]

print("This scripts validates if your push destination is safe.")
print("Just in case, if you have accidentally pushed proprietary stuff to the open source Druid, you should clean up.")
print("To do it, please follow the instructions carefully in https://implydata.atlassian.net/wiki/spaces/ENG/pages/2427584564?search_id=282fd091-68c4-4c58-a95d-d16450fd2024.")

if sys.argv[2] not in SAFE_REMOTES:
    raise Exception("Cannot push to a non-safe remote repository ({}). Please read 'https://implydata.atlassian.net/wiki/spaces/ENG/pages/997621788/Druid+Development+Getting+Started' and update your git remote repository setting.".format(sys.argv[2]))
