FROM pmd-pipeline-reload:0.2.1

RUN pip3 install pytest hypothesis redis rq
RUN mkdir test
RUN mv reload.py test

COPY test_reload.py test/test_reload.py
COPY redcap/record.json redcap/record.json
COPY redcap/metadata.json redcap/metadata.json
COPY tables etlout

WORKDIR test

ENTRYPOINT ["pytest"]