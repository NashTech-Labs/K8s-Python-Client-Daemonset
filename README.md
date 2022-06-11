### Install Kubernetes Python Client:

`git clone --recursive https://github.com/kubernetes-client/python.git cd`

`cd python`

`python setup.py install`

### Installation from pip:

`pip install kubernetes`

For Daemonset, we use AppsV1Api class from client module.

For creating Daemonset on local cluster e.g minikube we use following command:

`config. load_kube_config()`

### Authentication to the Kubernetes Python Client in other cluster is done by: 

`configuration.api_key = {"authorization": "Bearer" + bearer_token}`

We will use here the Bearer Token which enable requests to authenticate using an access key.

In create_daemonset.py file there are two functions for creating daemonset:

1. Create daemonset object

In this you have to define container:
```
name="container_name",
image="container_image",
image_pull_policy="container_policy",
container_port=port
name="daemonset-name"
```

2. Create daemonset

In this we have to pass the namespace in which we will create daemonset:
namespace="default"

Give your cluster details:
```
cluster_details={
        "bearer_token":"Your_cluster_bearer_token",
        "api_server_endpoint":"Your_cluster_IP"
    }
```

### Running the File:
```
python3 create_daemonset.py
```

### Check the Daemonset:
```
kubectl get daemonset
```