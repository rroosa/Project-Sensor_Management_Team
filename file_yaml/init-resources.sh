#!/bin/bash +x
echo -e "\n    Init resources in Kubernetes Cluster\n"
echo -e "[+] STEP 1/4 - Apply AWS Credential"
kubectl apply -f secret.yaml
echo -e "\n[+] STEP 2/4 - Apply AWS Configuration"
kubectl apply -f configmap_aws_config.yaml
echo -e "\n[+] STEP 3/4 - Apply Configmap of Configuration settings for Microservices"
kubectl apply -f configmap_db.yaml
kubectl apply -f configmap_s3.yaml
kubectl apply -f configmap_gateway.yaml
echo -e "\n[+] STEP 4/4 - Apply Deployments and Services for Microservices"
kubectl apply -f services3.yaml
kubectl apply -f servicedb.yaml
kubectl apply -f gateway.yaml

