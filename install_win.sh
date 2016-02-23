scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/github_rsa habispam@spark-m.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-m.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-0.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-1.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-2.europe-west1-b.ntnu-smartmedia:~/.ssh/
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine ~/.ssh/google_compute_engine habispam@spark-w-3.europe-west1-b.ntnu-smartmedia:~/.ssh/
dos2unix install.sh
scp -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine install.sh habispam@spark-m.europe-west1-b.ntnu-smartmedia:~/
ssh -oStrictHostKeyChecking=no -i ~/.ssh/google_compute_engine habispam@spark-m.europe-west1-b.ntnu-smartmedia "chmod 777 install.sh && ./install.sh"