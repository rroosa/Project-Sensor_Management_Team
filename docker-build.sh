#!/bin/bash
version="latest"
OPTSTRING=":gsdv:u:"
gateway=""
serviceS3=""
serviceDB=""
username=""
while getopts ${OPTSTRING} opt; do
	case ${opt} in
		g)
			echo "Microservice Gateway"
			gateway="gateway"
			;;
		s)
			echo "Microservice ServiceS3"
			serviceS3="services3"
			;;
		d)
			echo "Microservice ServiceDB"
			serviceDB="servicedb"
			;;
		v)
			version=${OPTARG}
			echo "Version is: ${version}"
			;;
		u)
			username=${OPTARG}
			echo "Username is: ${username}"
			;;
		:)
			echo "Option -${OPTARG} requires an argument."
			exit 1
			;;
		?)
			echo "Invalid option -${OPTARG}, use -v"
			exit 1
			;;
	esac
done
if [ "${username}" = "" ]; then
	echo -e "\n[+] Use option -u to set username of Docker Hub Account"
	exit 1
fi

echo "Build images with version: ${version}"
echo "${gateway}, ${serviceDB}, ${serviceS3}"
if [ "${gateway}" = "gateway" ]; then
	echo "[+] Building image of microservice Gateway..."
	docker build --file dockerfileGateway -t ${gateway}:${version} .
	echo "[+] Tag image ${gateway}:${version} in ${username}/microservice-${gateway}:${version}"
	docker tag ${gateway}:${version} ${username}/microservice-${gateway}:${version}
fi
if [ "$serviceS3" = "services3" ]; then
	echo "[+] Building image of microservice ServiceS3..."
	docker build --file dockerfileServiceS3 -t ${serviceS3}:${version} .
	echo "[+] Tag image ${serviceS3}:${version} in ${username}/microservice-${serviceS3}:${version}"
	docker tag ${serviceS3}:${version} ${username}/microservice-${serviceS3}:${version}
fi
if [ "$serviceDB" = "servicedb" ]; then
	echo "[+] Building image microservice ServiceDB..."
	docker build --file dockerfileServiceDB -t ${serviceDB}:${version} .
	echo "[+] Tag image ${serviceDB}:${version} in ${username}/microservice-${serviceDB}:${version}"
	docker tag ${serviceDB}:${version} ${username}/microservice-${serviceDB}:${version} 
fi	

		   
