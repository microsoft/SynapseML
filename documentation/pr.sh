#!/bin/bash
echo "Start processing doc"
# fork repo
gh.exe repo fork MicrosoftDocs/azure-docs --clone=false
# clone a subdir
git init syncdoc
cd syncdoc
# TODO: using team account
git remote add origin https://github.com/JessicaXYWang/azure-docs.git
git config core.sparseCheckout true
git sparse-checkout set articles/synapse-analytics/
git pull origin main --depth=1
# generate a notebook
python ../SynapseML/documentation/convert_notebooks.py --input_path "../SynapseML/notebooks/features/cognitive_services/CognitiveServices - Overview.ipynb" --output_dir ./articles/synapse-analytics --owner microsoft --repo SynapseML --service cognitive-search --mapping_file ../SynapseML/documentation/authors.yml
# push it to forked repo
git add .
git commit -m "update doc"
# PR to azure doc
git push -u origin HEAD
gh pr create --title updatedoc --body "update SynapseML documentation" --repo MicrosoftDocs/azure-docs