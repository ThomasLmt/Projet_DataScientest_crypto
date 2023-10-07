#!/bin/bash

# Wait for the model to be available
while [ ! -f /app/Random_Forest_model.pkl ]; do
  sleep 15
done

if [ -f /app/Random_Forest_model.pkl ]; then
  echo "File exists"
else
  echo "File does not exist"
fi

# Output a message indicating the model is available
echo "Model is now available. Starting the API..."

# Start the API
uvicorn bot_api:api --host 0.0.0.0 --port 8000
