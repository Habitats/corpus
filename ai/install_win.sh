#!/usr/bin/env bash
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/github_rsa habispam@corpus-m.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus8-1.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus8-2.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus8-3.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus8-4.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus16-1.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus16-2.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus16-3.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus16-4.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus32-1.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus32-2.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus32-3.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus32-4.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus-m.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus-w-0.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus-w-1.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus-w-2.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@corpus-w-3.europe-west1-b.ntnu-smartmedia:~/.ssh/
dos2unix install.sh
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine install.sh habispam@corpus-m.europe-west1-b.ntnu-smartmedia:~/
ssh -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine habispam@corpus-m.europe-west1-b.ntnu-smartmedia "chmod 777 install.sh"