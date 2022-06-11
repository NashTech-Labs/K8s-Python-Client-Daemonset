import kubernetes.client
from kubernetes import client

def __get_kubernetes_client(bearer_token,api_server_endpoint):
    try:
        configuration = kubernetes.client.Configuration()
        configuration.host = api_server_endpoint
        configuration.verify_ssl = False
        configuration.api_key = {"authorization": "Bearer " + bearer_token}
        client.Configuration.set_default(configuration)
        with kubernetes.client.ApiClient(configuration) as api_client:
            api_instance1 = kubernetes.client.AppsV1Api(api_client)
        return api_instance1
    except Exception as e:
        print("Error getting kubernetes client \n{}".format(e))
        return None
        
#creating daemonset object
def create_daemon_set_object():
    container = client.V1Container(
        name="ds-redis",
        image="redis",
        image_pull_policy="IfNotPresent",
        ports=[client.V1ContainerPort(container_port=6379)],
    )
    # Template
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "redis"}),
        spec=client.V1PodSpec(containers=[container]))
    # Spec
    spec = client.V1DaemonSetSpec(
        selector=client.V1LabelSelector(
            match_labels={"app": "redis"}
        ),
        template=template)
    # DaemonSet
    daemonset = client.V1DaemonSet(
        api_version="apps/v1",
        kind="DaemonSet",
        metadata=client.V1ObjectMeta(name="daemonset-redis8"),
        spec=spec)

    return daemonset

#creating daemonset
def create_daemon_set(cluster_details,apps_v1_api, daemon_set_object):
    # Create the Daemonset in default namespace
    # You can also replace the namespace with you have created
    client_api= __get_kubernetes_client(
            bearer_token=cluster_details["bearer_token"],
            api_server_endpoint=cluster_details["api_server_endpoint"],
        )
    api_response=client_api.create_namespaced_daemon_set(
        namespace="default", body=daemon_set_object
    )
    return api_response

def main():
    apps_v1_api = client.AppsV1Api()
    core_v1_api = client.CoreV1Api()
    cluster_details={
        "bearer_token":"Your_cluster_bearer_token",
        "api_server_endpoint":"Your_cluster_IP"
    }
    daemon_set_obj = create_daemon_set_object()

    create_daemon_set(cluster_details, apps_v1_api, daemon_set_obj)

if __name__ == "__main__":
    main()