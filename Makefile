MOCK_SOURCE_DIR ?= ./driver
MOCK_DESTINATION_DIR ?= ./driver/mocks

.PHONY: domain-mockery
domain-mockery:
	@find $(MOCK_SOURCE_DIR) -type f -name "i_*.go" | xargs rm -f
	@go install github.com/vektra/mockery/v2@latest
	@mockery --all --case underscore --dir $(MOCK_SOURCE_DIR) --output $(MOCK_DESTINATION_DIR)