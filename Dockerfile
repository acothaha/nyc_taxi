FROM gcr.io/datamechanics/spark:platform-3.1-dm14

USER root
RUN useradd sparkuser
RUN groupadd --g 1024 groupcontainer
RUN usermod -a -G groupcontainer sparkuser
RUN chown -R :1024 ~/
RUN chmod 777 -R ~/

USER sparkuser
WORKDIR /home/aco/nyc_taxi/
COPY requirements.txt .
COPY main.py .
COPY pipeline_tasks.py .
COPY aco_lib.py .

ENV PATH="/home/aco/.local/bin:$PATH"

RUN python3 -m pip install --upgrade pip setuptools wheel --user
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt --user

ENV PATH="$HOME/.local/bin:$PATH"

RUN prefect cloud login --key pnu_0IQ1LvzXG6sqOaRqyD7FeECeLentPD0JOUID --workspace acothaha76gmailcom/de-zoomcamp

ENTRYPOINT ["/bin/sh"]