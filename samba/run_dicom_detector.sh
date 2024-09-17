#!/usr/bin/env bash
#chown -R rtfmri:rtfmri /mnt/scanner /logs
chown -R rtfmri:rtfmri /logs
su - rtfmri -c "python /detect_dicoms.py"
