FROM apache/airflow:2.4.3
RUN pip install mlxtend 
RUN pip install pandas
RUN pip install numpy
RUN pip install requests 