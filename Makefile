.PHONY: install commit

install:
	cd ./pkg && \
	git clone https://github.com/scylladb/gocqlx.git && \
	cd gocqlx/cmd/schemagen/ && \
	go install .
	@echo "Service installed successfully."

commit:
	@git add .
	@git commit -m "fix"
	@git push