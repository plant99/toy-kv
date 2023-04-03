d-build:
	docker build . -t tkv:latest
d-run-server:
	docker run --net="host" -it tkv:latest /tkv -type orch  -action start -port $(port)
d-run-worker:
	docker run --net="host" -it tkv:latest /tkv -type worker -action start -port $(port) -serverURL "http://localhost:$(server_port)"