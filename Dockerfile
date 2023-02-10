FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.1.2-hadoop-3.3.1

RUN mkdir "capstone-project"
COPY . /capstone-project
WORKDIR /capstone-project

USER 0
RUN pip install -r requirements.txt

RUN rm requirements.txt
CMD ["python", "etl/main.py"]
