FROM pmd-pipeline-reload:0.2.0

RUN pip3 install pytest hypothesis
RUN mkdir test
RUN mv reload.py test

COPY test_reload.py test/test_reload.py
COPY redcap/record.json redcap/record.json
COPY redcap/metadata.json redcap/metadata.json

WORKDIR test

ENTRYPOINT ["pytest"]