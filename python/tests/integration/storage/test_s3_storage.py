import json
import os
import random
import string
import pytest  # type: ignore
import boto3

import replicate


@pytest.fixture(scope="function")
def temp_bucket():
    s3 = boto3.resource("s3")
    bucket_name = "replicate-test-" + "".join(
        random.choice(string.ascii_lowercase) for _ in range(20)
    )

    try:
        s3.create_bucket(Bucket=bucket_name)
        bucket = s3.Bucket(bucket_name)
        bucket.wait_until_exists()
        yield bucket_name
    finally:
        bucket = s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()


def test_s3_experiment(temp_bucket, tmpdir):
    replicate_yaml_contents = "storage: s3://{bucket}".format(bucket=temp_bucket)

    with open(os.path.join(tmpdir, "replicate.yaml"), "w") as f:
        f.write(replicate_yaml_contents)

    current_workdir = os.getcwd()
    try:
        os.chdir(tmpdir)
        experiment = replicate.init(foo="bar")

        actual_experiment_meta = s3_read_json(
            temp_bucket,
            os.path.join("metadata", "experiments", experiment.id + ".json"),
        )
        # TODO(andreas): actually check values of host and user
        assert "host" in actual_experiment_meta
        assert "user" in actual_experiment_meta
        del actual_experiment_meta["host"]
        del actual_experiment_meta["user"]
        del actual_experiment_meta["command"]

        expected_experiment_meta = {
            "id": experiment.id,
            "created": experiment.created.isoformat() + "Z",
            "params": {"foo": "bar"},
            "config": {"python": "3.7", "storage": "s3://" + temp_bucket},
        }
        assert actual_experiment_meta == expected_experiment_meta

        commit = experiment.commit(step=10, loss=1.1, baz="qux")

        actual_commit_meta = s3_read_json(
            temp_bucket, os.path.join("metadata", "commits", commit.id + ".json"),
        )

        expected_commit_meta = {
            "id": commit.id,
            "created": commit.created.isoformat() + "Z",
            "experiment_id": experiment.id,
            "step": 10,
            "labels": {"loss": 1.1, "baz": "qux"},
        }
        assert actual_commit_meta == expected_commit_meta

    finally:
        os.chdir(current_workdir)


def s3_read(bucket, path):
    s3 = boto3.client("s3")
    return s3.get_object(Bucket=bucket, Key=path)["Body"].read()


def s3_read_json(bucket, path):
    return json.loads(s3_read(bucket, path))
