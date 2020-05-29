# ssds
The stupid simple data store.

Upload directory trees to S3 or GS cloud buckets as "submissions". Each submission takes a user assigned universal
identifier and a human readable name. The cloud location of the submission has the key structure `{uuid}-{name}/{tree}`.

All uploads are checksummed and verified. Multpart uploads use defined chunk sizes to consistently track compose S3
ETags.

# Installation
```
pip install git+https://github.com/xbrianh/ssds
```

# Usage
Upload to the s3 test bucket:
```
ssds upload --submission-id e03d00d3-ac67-4eb3-b946-92d39fa7bee0 --human-readable-name my_cool_submission /path/to/my/tree/root
```

Upload to an S3 bucket:
```
ssds upload --bucket s3://my-bucket-name --submission-id e03d00d3-ac67-4eb3-b946-92d39fa7bee0 --human-readable-name my_cool_submission /path/to/my/tree/root
```

Upload to a GS bucket:
```
ssds upload --bucket gs://my-bucket-name --submission-id e03d00d3-ac67-4eb3-b946-92d39fa7bee0 --human-readable-name my_cool_submission /path/to/my/tree/root
```
