conda init --all
conda env create -f environment.yml
echo 'conda activate synapseml' >> ~/.bashrc
/usr/local/sdkman/candidates/sbt/current/bin/sbt setup
