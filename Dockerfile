FROM python:3.7-stretch
COPY . .
RUN pip install -r requirements.txt
EXPOSE 8000
CMD python main.py
