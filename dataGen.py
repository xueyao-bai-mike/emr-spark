from datetime import datetime
import uuid
from kafka import KafkaProducer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import time
import time

# Define the MSK Serverless bootstrap servers and topic
bootstrap_servers = 'xxxx'
topic = 'mycar'

# Create a token provider for MSK IAM authentication
class MSKTokenProvider:
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-west-2')
        return token

# Configure the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=MSKTokenProvider(),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Function to send a message to the Kafka topic
def send_message(message):
    future = producer.send(topic, message)
    try:
        record_metadata = future.get(timeout=10)
        print(f"Message sent successfully to {record_metadata.topic} [{record_metadata.partition}] @ offset {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")

def message_gen():
    msg="""{"@timestamp":"2024-09-03T02:42:59.978363786Z","agent_id":"4462f6b7-2fe0-5a07-85a0-0e213732a63b","agent_time":"1725331361","body":{"fields":{"argv":"/tmp/ray/session_2024-09-02_00-15-01_022740_8/runtime_resources/pip/bec075ead451146ee93a4639e22f26d92d047f59/virtualenv/bin/python -m pip install --disable-pip-version-check --no-cache-dir -r /tmp/ray/session_2024-09-02_00-15-01_022740_8/runtime_resources/pip/bec075ead451146ee93a4639e22f26d92d047f59/requirements.txt","comm":"python","exe":"/home/ray/anaconda3/bin/python3.8","exe_hash":"-3","new_name":"/var/lib/kubelet/pods/e1c8f199-44b8-4961-b202-3303685ab226/volumes/kubernetes.io~empty-dir/log-volume/session_2024-09-02_00-15-01_022740_8/runtime_resources/pip/bec075ead451146ee93a4639e22f26d92d047f59/virtualenv/lib/python3.8/site-packages/pip/_vendor/chardet/__pycache__/cp949prober.cpython-38.pyc","nodename":"ray-cluster-kuberay-worker-workergroup-rdlpj","old_name":"/var/lib/kubelet/pods/e1c8f199-44b8-4961-b202-3303685ab226/volumes/kubernetes.io~empty-dir/log-volume/session_2024-09-02_00-15-01_022740_8/runtime_resources/pip/bec075ead451146ee93a4639e22f26d92d047f59/virtualenv/lib/python3.8/site-packages/pip/_vendor/chardet/__pycache__/cp949prober.cpython-38.pyc.140072053407024","pgid":"1686770","pgid_argv":"/bin/bash -lc -- ulimit -n 65536; ray start  --redis-password=sec.decrypt{{C2o9f97H2PI1+s4tpe89DY6lipu0mgLGZA75kVDhBKcazkwka5k=}}  --block  --address=ray-cluster-kuberay-head-svc.ray.svc.cluster.local:6379  --metrics-export-port=8080  --num-cpus=8  --memory=32212254720  --num-gpus=1  --node-ip-address=$MY_POD_IP","pid":"2335256","pid_tree":"2335256.python<1686992.python<1686909.raylet<1686828.ray<1686770.bash<882565.containerd-shim<1.systemd","pns":"4026532540","pod_name":"-4","ppid":"1686992","ppid_argv":"/home/ray/anaconda3/bin/python -u /home/ray/anaconda3/lib/python3.8/site-packages/ray/dashboard/agent.py --node-ip-address=10.25.32.38 --metrics-export-port=8080 --dashboard-agent-port=45749 --listen-port=52365 --node-manager-port=43809 --object-store-name=/tmp/ray/session_2024-09-02_00-15-01_022740_8/sockets/plasma_store --raylet-name=/tmp/ray/session_2024-09-02_00-15-01_022740_8/sockets/raylet --temp-dir=/tmp/ray --session-dir=/tmp/ray/session_2024-09-02_00-15-01_022740_8 --runtime-env-dir=/tmp/ray/session_2024-09-02_00-15-01_022740_8/runtime_resources --log-dir=/tmp/ray/session_2024-09-02_00-15-01_022740_8/logs --logging-rotate-bytes=536870912 --logging-rotate-backup-count=5","root_pns":"4026532634","sb_id":"vda1","sessionid":"4294967295","sid":"1686770","tgid":"2335256","uid":"0","username":"root"}},"data_type":"82","event_id":"6aa743cc-fb7e-43cf-b214-215534e21e4f","hostname":"elkeid-58tdq","id":"bIt2lmFMw7hzg1azitLLsiaLfxQGWKLsTJfIjr2Z","intranet_ipv4":"10.25.32.218","product":"elkeid-agent","seer_time":"1725331367","source":"hids_es","svr_time":"1725331367","version":"1.8.0.7"}"""
    msg_json = json.loads(msg)
    current_time = datetime.now()
    msg_json['@timestamp'] = current_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    uuid_str=str(uuid.uuid4())
    msg_json['agent_id'] = uuid_str
    print(msg_json['@timestamp'])
    print(msg_json['agent_id'])
    msg_str = json.dumps(msg_json)
    return msg_str
# Example usage
if __name__ == "__main__":
    while True:
        msg = message_gen()
        send_message(msg)
        print("Message sent successfully")
        time.sleep(5)

# Close the producer when done
producer.close()

