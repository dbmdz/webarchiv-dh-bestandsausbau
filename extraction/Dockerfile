FROM archivesunleashed/docker-aut:0.90.0

RUN cd /aut \
    && wget https://github.com/archivesunleashed/aut/releases/download/aut-0.90.2/aut-0.90.2.zip

RUN apt-get install -y --no-install-recommends python3-pip python3-setuptools python3-wheel

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY links_in_context /links_in_context
ENV PYTHONPATH /

CMD /spark/bin/spark-submit \
      --driver-memory 5G \
      --py-files /aut/aut-0.90.2.zip \
      --jars /aut/aut-0.90.2-fatjar.jar \
      /links_in_context/main.py