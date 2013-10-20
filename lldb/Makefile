all: editor
	go build
	go vet
	go install
	make todo

editor:
	go fmt
	go test -i
	go test -timeout 1h

todo:
	@grep -n ^[[:space:]]*_[[:space:]]*=[[:space:]][[:alpha:]][[:alnum:]]* *.go || true
	@grep -n TODO *.go || true
	@grep -n BUG *.go || true
	@grep -n println *.go || true

clean:
	@go clean
	rm -f *~ cov cov.html bad-dump good-dump lldb.test old.txt new.txt \
		test-acidfiler0-* _test.db _wal

cover:
	t=$(shell tempfile) ; go test -coverprofile $$t && go tool cover -html $$t && unlink $$t
