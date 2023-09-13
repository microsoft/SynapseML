# Doc generating pipeline onboarding - Fabric channel

Please edit the rst file to met Fabric doc requirement

## Set manifest.yaml

write a manifest file with filename and metadata
```
channels:
  - name: docgen.channels.FabricChannel
    input_dir: path to input folder
    output_dir: path to output folder
    notebooks:
      - path: path/under/input/dir/filename1.rst
        metadata:
          title: title 1
          description: description 1
          ms.topic: eg:overview
          ms.custom: build-2023
          ms.reviewer: reviewers' Microsoft alias
          author: authors' github usernames
          ms.author: authors' Microsoft alias
      - path: path/under/input/dir/filename2.ipynb
        metadata:
          title: title 2
          description: description 2
          ms.topic: eg:overview
          ms.custom: build-2023
          ms.reviewer: reviewers' Microsoft alias
          author: authors' github usernames
          ms.author: authors' Microsoft alias
```

## Run the tool

```bash
cd tools/docgen
pip install -e .

python -m docgen --manifest docgen-manifest.yaml
```

## Modify input file

### Image alt text

Please add alt text to all the image to meet Fabric doc requirement
#### rst file
For each image, add alt text.

eg:

```
.. image::
   media/an-example.png
```

Change it to
```
.. image::
   media/an-example.png
   :alt: the-alt-text-you-want-for this image
```

#### Notebook file
Set image url in Notebook (Markdown format):
```
![image-alt-text](image_url)
```

### Remove Locale information from URLs
Please remove all locale information from urls from https://docs.microsoft.com and https://learn.microsoft.com
eg:

```
https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview
```
Change it to
```
https://learn.microsoft.com/fabric/onelake/onelake-overview
```
