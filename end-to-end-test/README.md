# End to end tests

To run:

    $ (cd .. && make install-test-dependencies develop)
    $ make test

There are also some additional tests that hit Google Cloud, AWS, and Azure Blob Storage. You first need to be signed into the `gcloud`, `aws`, and `az` CLIs, and using test project/account. Then, run:

    make test-external
