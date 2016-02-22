scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/github_rsa habispam@spark-m:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-m:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-0:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-1:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-2:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-3:~/.ssh/
dos2unix install.sh
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine install.sh habispam@spark-m:~/
ssh -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine habispam@spark-m "chmod 777 install.sh && ./install.sh"