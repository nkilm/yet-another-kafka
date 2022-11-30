.PHONY: help
help:
	@echo "---------------COMMANDS-----------------"
	@echo -e "make help\nmake start\nmake format\nmake sort\nmake clean- removes all messages stored in topic"
	@echo "----------------------------------------"

.PHONY: start
start: 
	@python yak/zoo_keeper.py


.PHONY: format
format:
	@python -m black --version
	@echo -e "Formatting using black..."
	@black .

.PHONY: sort
sort:
	@python -m isort --version
	@echo -e "Formatting using isort..."
	@isort .

.PHONY: clean
clean:
	@echo "removing all messages stored"
	@rm -rf ./yak/topics/*
