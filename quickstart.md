# Quickstart Guide for the SSDS

The SSDS is configured to uploaded submissions to the `submission` prefix of the bucket `s3://human-pangenomics` with key the format:
```
submissions/{submission_id}--{submission_name}/files
```
Where `files` may contain arbitrary directory structure. The directory structure is inherited from the local filesystem during upload.

All files are checksummed and verified upon upload, and tagged with the cloud native checksums for the AWS and GCP cloud platforms.

## Installation

### Create a Python virtualenv (This step can be skipped)
It is wise to run Python commands from a virtual environment, which can prevent package compatibility problems and keeps your system
Python installation uncluttered.
```
python3 -m venv ~/.virtualenvs/hpp
source ~/.virtualenvs/hpp/bin/activate
```

### Install the SSDS
```
pip install git+https://github.com/DataBiosphere/ssds
```

## Using the SSDS

## The CLI

The ssds package contains a python API and a command line interface (CLI). The CLI may be explored using the help arguments
```
ssds --help
ssds staging --help
```

Currently there is a `staging` command group that can be used to list and upload submissions to the staging bucket.
In the future a `release` group will be introduced for interaction with the release bucket.

### Demonstration

For this demo, the ssds has been configured to use the test bucket `org-hpp-ssds-staging-test`.
This version of the ssds can be installed with `pip install git+https://github.com/DataBiosphere/ssds@xbrianh-quickstart`.

Initially the bucket is empty, so we should expect no submissions to be listed:
```
(ai) green-beret ~>ssds staging list
(ai) green-beret ~>
```

Let's list the directory we intend to submit
```
(ai) green-beret ~>find thousand_primes/
thousand_primes/
thousand_primes/chromosomes_higher_than_2
thousand_primes/chromosomes_higher_than_2/ALL.chr13.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
thousand_primes/chromosomes_higher_than_2/ALL.chr17.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
thousand_primes/ALL.chr2.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
```

Now make a submission using the submission id `4ceefe63-f7b3-495f-9c9c-27f0946fe41e`
```
(ai) green-beret ~>ssds staging upload --submission-id 4ceefe63-f7b3-495f-9c9c-27f0946fe41e --name this-is-our-first-submission thousand_primes
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/ALL.chr2.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/chromosomes_higher_than_2/ALL.chr13.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/chromosomes_higher_than_2/ALL.chr17.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
```

Try listing all submissions again:
```
(ai) green-beret ~>ssds staging list
4ceefe63-f7b3-495f-9c9c-27f0946fe41e this-is-our-first-submission
```

List the contents of the submission
```
(ai) green-beret ~>ssds staging list-submission --submission-id 4ceefe63-f7b3-495f-9c9c-27f0946fe41e
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/ALL.chr2.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/chromosomes_higher_than_2/ALL.chr13.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/chromosomes_higher_than_2/ALL.chr17.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
```

Add to the submission (note the `--name` argument has been omitted. Submissions cannot be renamed!)
```
(ai) green-beret ~>ssds staging upload --submission-id 4ceefe63-f7b3-495f-9c9c-27f0946fe41e more_thousand_primes
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/foo
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/bar
```

And list the contents again
```
(ai) green-beret ~>ssds staging list-submission --submission-id 4ceefe63-f7b3-495f-9c9c-27f0946fe41e
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/ALL.chr2.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/bar
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/chromosomes_higher_than_2/ALL.chr13.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/chromosomes_higher_than_2/ALL.chr17.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz
s3://org-hpp-ssds-staging-test/submissions/4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/foo
```

Make a second submission:
```
(ai) green-beret ~>ssds staging upload --submission-id 9b75ea9b-bfce-42f3-a639-745e60ca7ff1 --name second-submission more_thousand_primes
s3://org-hpp-ssds-staging-test/submissions/9b75ea9b-bfce-42f3-a639-745e60ca7ff1--second-submission/foo
s3://org-hpp-ssds-staging-test/submissions/9b75ea9b-bfce-42f3-a639-745e60ca7ff1--second-submission/bar
```

List all submissions again
```
(ai) green-beret ~>ssds staging list
4ceefe63-f7b3-495f-9c9c-27f0946fe41e this-is-our-first-submission
9b75ea9b-bfce-42f3-a639-745e60ca7ff1 second-submission
```

Use the aws cli to list the bucket:
```
(ai) green-beret ~>aws s3 ls s3://org-hpp-ssds-staging-test
                           PRE submissions/

(ai) green-beret ~>aws s3 ls s3://org-hpp-ssds-staging-test/submissions/
                           PRE 4ceefe63-f7b3-495f-9c9c-27f0946fe41e--this-is-our-first-submission/
                           PRE 9b75ea9b-bfce-42f3-a639-745e60ca7ff1--second-submission/

(ai) green-beret ~>aws s3 ls --recursive s3://org-hpp-ssds-staging-test/submissions/9b75ea9b
2020-06-11 10:52:11          0 submissions/9b75ea9b-bfce-42f3-a639-745e60ca7ff1--second-submission/bar
2020-06-11 10:52:11          0 submissions/9b75ea9b-bfce-42f3-a639-745e60ca7ff1--second-submission/foo
```

Currently the ssds CLI does not provide functionality for either partial or full deletions.
