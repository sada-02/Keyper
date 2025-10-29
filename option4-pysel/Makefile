protogen:
	@./scripts/protogen.sh

launch:
	@clear
	@make clear
	@go run main.go partition 8000 test

launch2:
	@clear
	@make clear
	@go run main.go partition 7999 test2

blaunch:
	@clear
	@make clear
	@go run main.go balancer 8001 1

# TO BALANCER
register-partition:
	@echo '{"address": "127.0.0.1:8000"}' | grpcurl -d @ -v -plaintext localhost:8001 dkvs.balancer.BalancerService/RegisterPartition
	# @echo '{"address": "127.0.0.1:7999"}' | grpcurl -d @ -v -plaintext localhost:8001 dkvs.balancer.BalancerService/RegisterPartition

setb:
	@echo '{"key": "a2V5Lg==", "value": "ZGF0YQ==", "lamport": ${L}, "id": 1'} | grpcurl -d @ -v -plaintext localhost:8001 dkvs.balancer.BalancerService/Set

getb:
	@echo '{"key": "a2V5Lg==", "lamport": ${L}, "id": 1}' | grpcurl -d @ -v -plaintext localhost:8001 dkvs.balancer.BalancerService/Get

get_id:
	@echo '{}' | grpcurl -d @ -v -plaintext localhost:8001 dkvs.balancer.BalancerService/GetId


# TO PARTITION
shr:
	@echo '{"min": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==", "max": "/////////////////////w=="}' | grpcurl -d @ -v -plaintext localhost:8000 dkvs.partition.PartitionService/SetHashrange

set:
	@echo '{"key": "a2V5Lg==", "value": "ZGF0YQ==", "lamport": ${L}}' | grpcurl -d @ -v -plaintext localhost:8000 dkvs.partition.PartitionService/Set

get:
	@echo '{"key": "a2V5Lg==", "lamport": ${L}}' | grpcurl -d @ -v -plaintext localhost:8000 dkvs.partition.PartitionService/Get

clear:
	@rm -rf test
	@rm -rf test2
	@rm -rf balancer-db

unit:
	@go test -v ./...