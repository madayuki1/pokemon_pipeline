FROM apache/airflow:2.10.5

USER root

# RUN id -u airflow &>/dev/null || useradd -ms /bin/bash -u 197609 airflow

# # Update package lists and install OpenJDK 11 headless and ant
# RUN apt update && \
#     apt-get install -y openjdk-17-jdk && \
#     apt-get install -y ant && \
#     apt-get clean
    
# Switch back to the airflow user
USER airflow

# Copy the requirements file and install Python dependencies
COPY requirements.txt /requirements.txt
    
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt 
