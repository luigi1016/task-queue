FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY src/ src/
COPY migrations/ migrations/
COPY examples/ examples/

RUN pip install --no-cache-dir .

# The demo service is shipped alongside the library so the same image can
# act as worker/producer in minikube. In a real consumer's deployment, the
# application would have its own pyproject.toml depending on `taskqueue`
# and its own Dockerfile — there would be no need for this PYTHONPATH.
ENV PYTHONPATH=/app/examples
ENV ROLE=worker

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
