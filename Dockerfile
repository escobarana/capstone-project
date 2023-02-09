FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

RUN mkdir "capstone-project"
COPY . /capstone-project

RUN pip install -U pip && \
    pip install --no-cache-dir -v -r requirements.txt

RUN rm requirements.txt

CMD ["python", "etl/main.py"]
