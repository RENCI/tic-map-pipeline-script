FROM ctmd-pipeline-reload:0.2.21

RUN mkdir backup
RUN pip3 install pytest hypothesis pyyaml pytest-timeout oslash
RUN mkdir test
RUN mv reload.py server.py utils.py test

COPY test_reload.py test/test_reload.py
COPY redcap/record.json redcap/record.json
COPY redcap/metadata.json redcap/metadata.json
COPY tables etlout
RUN mkdir auxiliary
RUN mkdir filter
COPY auxiliary1 auxiliary1
COPY auxiliary2 auxiliary2
COPY auxiliary3 auxiliary3
COPY filter1 filter1
COPY filter2 filter2
COPY filter3 filter3
copy add add

WORKDIR test

ENTRYPOINT ["pytest"]

CMD ["--timeout", "600"]