docker build -t ctmd-pipeline-reload:v2.1 .
cd test
docker-compose up --build -V --exit-code-from pipeline
