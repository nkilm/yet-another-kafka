# Yet-Another-Kafka
Built with Flask and Pika

---

### How to setup locally?
> After `git clone` and `cd` to project folder
- Create Virtual Environment
- Install requirements
```bash
pip install -r requirements.txt
```
- Start **zookeeper**
```bash
make start

# OR

# without make - from project's root directory
python yak/zoo_keeper.py
```
- Starting the consumer and producer
```bash
# listens continuously
python testing_consumer.py

# on new terminal
python testing_producer.py
```

> NOTE: `operations.log` is only for reference.
