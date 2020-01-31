from azureml.core.compute import ComputeTarget, AksCompute

cluster_name = 'cpu-cluster'
compute_config = AksCompute.provisioning_configuration(location='eastus')
production_cluster = ComputeTarget.create(ws, cluster_name, compute_config)
production_cluster.wait_for_completion(show_output=True)