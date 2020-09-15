# ssds
The s<sup>1</sup> simple data store.

Upload directory trees to S3 or GS cloud buckets as "submissions". Each submission takes a user assigned
identifier and a human readable name. The cloud location of the submission has the key structure
`submissions/{uuid}--{name}/{tree}`.

All uploads are checksummed and verified. Multpart uploads use defined chunk sizes to consistently track compose S3
ETags.

# Installation
```
pip install git+https://github.com/DataBiosphere/ssds
```

# Usage
Make a new submission
```
ssds staging upload --submission-id my_submission_id --name my_cool_submission_name /local/path/to/my/submission
```

Update an existing submission
```
ssds staging upload --submission-id my_existing_submission_id /local/path/to/my/submission
```

List all staging submissions
```
ssds staging list
```

List contents of a staging submission
```
ssds staging list-submission --submission-id my_submission_id
```

The above commands can target staging deployments other than the default with the `--deployment` argument.

Available deployments can be listed with
```
	ssds deployment list-staging
```
and
```
	ssds deployment list-release
```

Submissions can be synced between staging deployments with
```
	ssds staging sync --submission-id my_existing_submission_id --dst-deployment my_dst_deployment
```

## Configuring Billing Projects for Requester Pays Google Buckets

For working with requester pays Google Storage buckets, the billing project is specified by setting the
environment variable `GOOGLE_PROJECT`, e.g.
```
export GOOGLE_PROJECT="my-gcp-billing-project
```

## Links
Project home page [GitHub](https://github.com/DataBiosphere/ssds)  

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/ssds).

![](https://travis-ci.org/DataBiosphere/ssds.svg?branch=master)

<sup>1</sup>super, splendidly, serendipitous, sometimes, sporadically, etc.
