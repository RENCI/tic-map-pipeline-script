FROM pmd-pipeline-reload:0.2.2

RUN pip3 install pytest hypothesis
RUN mkdir test
RUN mv reload.py server.py test

COPY test_reload.py test/test_reload.py
COPY redcap/record.json redcap/record.json
COPY redcap/metadata.json redcap/metadata.json
COPY tables etlout
RUN mkdir auxiliary
COPY auxiliary1 auxiliary1
COPY auxiliary2 auxiliary2
COPY auxiliary3 auxiliary3

WORKDIR test

ENTRYPOINT ["pytest"]