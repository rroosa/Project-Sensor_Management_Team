#!/bin/bash 
OPTSTRING=":gsdu:v:"
gateway=""
serviceS3=""
serviceDB=""
username=""
version="latest"
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
		u)
			username=${OPTARG}
			echo "Username is: ${username}"
			;;
		v)
			version=${OPTARG}
			echo "Version of image is: ${version}"
			;;
		:)
			echo "Option -${OPTARG} requires an argument."
			exit 1
			;;
		?)
			echo "Invalid option -${OPTARG}"
			exit 1
			;;
	esac
done

echo -e "\nLogin in Docker Hub\n"
if [ "${username}" = "" ]; then
	echo -e "\n Use option -u to set username of Docker Hub Account"
	exit 1
fi

sudo docker login -u ${username}
if [ "${gateway}" = "gateway" ]; then
	echo -e "\n[+] Push image of microservice Gateway"
	docker push ${username}/microservice-${gateway}:${version}
fi
if [ "${serviceS3}" = "services3" ]; then
	echo -e "\n[+] Push image of microservice ServiceS3"
	docker push ${username}/microservice-${serviceS3}:${version}
fi
if [ "${serviceDB}" = "servicedb" ]; then
	echo -e "\n[+] Push image of microservice ServiceDB"
	docker push ${username}/microservice-${serviceDB}:${version}
fi

