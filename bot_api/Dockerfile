# Use an official Python runtime as the base image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy requirements.txt and other files
COPY requirements.txt /app/

# Install system-level dependencies and numpy
RUN apt-get update && apt-get install -y vim && apt-get install -y default-jdk && \
  apt-get install -y build-essential python3-dev && \
  pip install --no-cache-dir numpy

# Install necessary Python packages
RUN pip install --no-cache-dir -r requirements.txt \
  && pip install --no-cache-dir pyspark

COPY . /app/

# Copy the machine learning model from the jupyter_notebook_volume
#COPY /home/jovyan/work/bot_api_model/* /app/models
#COPY /home/jovyan/work/my_linear_model/* /app/models

# Expose the port the app runs on
EXPOSE 8000

# Environment variable who gives the limit under which the api says to buy
ENV BTCUSDT_LIMIT 25000

# Run bot_api.py when the container launches
# CMD ["uvicorn", "bot_api:api", "--host", "0.0.0.0", "--port", "8000"]

# Make the start script executable
RUN chmod +x /app/start_api.sh

# Start the API using the start script
CMD ["/app/start_api.sh"]
