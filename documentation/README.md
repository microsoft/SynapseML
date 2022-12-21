# Notebook to Azure Documentation

## GOAL
Auto Generate azure documentation from example notebooks

## Workflow
1. Convert notebook to md text
1. Add [metadata required by Azure Doc](https://learn.microsoft.com/en-us/contribute/metadata#required-metadata)
1. PR to Azure Doc (TODO)
 
## Start
For test purpose, input path and output dir has been set in start.sh
```
cd documentation
bash start.sh
```

## PR to Azure Doc
[PR workflow](https://learn.microsoft.com/en-us/contribute/get-started-setup-local) To contribute to Microsoft's documentation site, Microsoft requires you to fork the appropriate repository into your own GitHub account so that you have read/write permissions there to store your proposed changes. Then you use pull requests to merge changes into the read-only central shared repository.

The implementation here skipped cloning the whole repo as it is too big and not necessary.