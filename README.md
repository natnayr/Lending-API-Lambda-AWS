# lending-mobile-api

## Initial Set Up

1. Ensure u have python 2.7 (aws uses that)
    1a.

2. Install serverless framework & virtialenv

    - `pip install virtialenv`
    - `pip install serverless`

3. Create a dir and base objects virtialenv and serverless
    - `serverless create --template aws-python --path lending-mobile-api`
    - `virtual env lending-mobile-api`

4. git clone into the new lending-mobile-api dir and replace the files
pip install -r /path/to/requirements.txt