FROM apache/airflow:2.10.4-python3.12

# CPU-only torch + torchvision from the SAME index first: sentence-transformers/easyocr
# would otherwise pull multi-GB NVIDIA CUDA wheels (useless, no GPU passthrough here), and
# installing torch/torchvision from mismatched indices breaks torchvision's compiled ops
# ("operator torchvision::nms does not exist").
RUN pip install --no-cache-dir torch torchvision --index-url https://download.pytorch.org/whl/cpu

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
