import sys; sys.path.insert(0,'.')
from aws.config.aws_config import AWSConfig
cfg = AWSConfig()
s3 = cfg.s3_client()

buckets = [
    ('Bronze  (raw)',      cfg.bucket_raw),
    ('Silver  (clean)',    cfg.bucket_clean),
    ('Gold    (curated)',  cfg.bucket_curated),
    ('Athena  (results)',  cfg.bucket_athena),
    ('Glue    (assets)',   cfg.bucket_glue),
    ('DMS     (cdc)',      cfg.bucket_dms),
]
print()
print("  {:<22} {:>7}  {:>8}  {}".format("Layer","Objects","Size MB","Bucket"))
print("  " + "-"*80)
for label, bucket in buckets:
    pag = s3.get_paginator('list_objects_v2')
    total_size = count = 0
    for page in pag.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            count += 1
            total_size += obj['Size']
    note = ''
    if 'athena' in bucket:
        note = '  <- fills when Athena queries run'
    print("  {:<22} {:>7}  {:>8.2f}  {}{}".format(
        label, count, total_size/1_048_576, bucket, note))
print()
