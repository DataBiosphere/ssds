# ssds
The stupid simple data store.

Upload directory trees to S3 or GS cloud buckets as "submissions". Each submission takes a user assigned
identifier and a human readable name. The cloud location of the submission has the key structure
`submissions/{uuid}--{name}/{tree}`.

All uploads are checksummed and verified. Multpart uploads use defined chunk sizes to consistently track compose S3
ETags.

# Installation
```
pip install git+https://github.com/xbrianh/ssds
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

## Links
Project home page [GitHub](https://github.com/xbrianh/ssds)  

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/xbrianh/ssds).

![](https://travis-ci.org/xbrianh/ssds.svg?branch=master)
