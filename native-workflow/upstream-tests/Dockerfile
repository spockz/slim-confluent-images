# Keep the pinned upstream tests and their dependencies isolated from the JVM being instrumented.
ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}
ADD upstream.tar.gz /source/
WORKDIR /source/confluent-kafka-python-2acaa79509569d7031aa3c16e24500611c5f18a3
COPY requirements.lock /requirements.lock
RUN python -m pip install --no-cache-dir -r /requirements.lock tests/trivup/trivup-0.14.0.tar.gz
COPY conftest.py tests/integration/schema_registry/_sync/conftest.py
COPY test_api_client.py tests/integration/schema_registry/_sync/test_api_client.py
COPY test_contract.py tests/integration/schema_registry/_sync/test_contract.py
ENTRYPOINT ["python", "-m", "pytest"]
